using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BFTCRDT.Client;

public class BenchmarkRunners
{   
    protected string[] serverAddresses;
    protected int duration;
    protected int clientThreads;
    protected string[] keys;

    // number of messages between send / receive
    protected bool limitSendRate;
    protected int targetOutputTPS;
    protected int perThreadOutputBatch;
    protected readonly int threadSleepInterval = 50;
    
    private bool supressRunningStatus = false;

    protected WorkloadGenerator workloadGenerator;

    // used for worker threads to stop
    private CancellationTokenSource  running = new CancellationTokenSource ();


    public BenchmarkRunners(BenchmarkConfig benchmarkConfig)
    {
        this.serverAddresses = benchmarkConfig.addresses;
        this.duration = benchmarkConfig.duration;
        this.clientThreads = benchmarkConfig.clientThreads;
        this.workloadGenerator = GetWorkloadByTypeCode(benchmarkConfig.typeCode, benchmarkConfig.numObjs, benchmarkConfig.opsRatio, benchmarkConfig.safeRatio);
        this.targetOutputTPS = benchmarkConfig.targetOutputTPS;

    }

    public void Init()
    {
        if (targetOutputTPS > 0)
        {
            perThreadOutputBatch = (targetOutputTPS / clientThreads ) / (1000 / threadSleepInterval);
            perThreadOutputBatch = Math.Max(1, perThreadOutputBatch);
        }
        else
        {
            perThreadOutputBatch = 0;
        }

        Console.WriteLine("Initializing objects...");

        List<ServerConnection> conns = new();

        // create connection to each server
        foreach (string address in serverAddresses)
        {
            // Parse ip and port
            ServerConnection conn = new(address.Split(":")[0], int.Parse(address.Split(":")[1]));
            if (!conn.Connect())
                throw new Exception($"Sever connection fails");
            conns.Add(conn);
        }

        // create object on the first server
        keys = workloadGenerator.InitObj(conns[0]);

        Thread.Sleep(5000);

        bool flag = true;
        foreach (var conn in conns)
        {
            flag = flag && workloadGenerator.VerifyCreate(conn);
            conn.Close();
        }

        if (!flag)
        {
            throw new Exception("Failed to create object");
        }

    }

    private Barrier barrier;
    private Barrier barrier2;
    public List<Result> Run(bool supressRunningStatus = false)
    {
        this.supressRunningStatus = supressRunningStatus;

        List<Thread> threads = new();
        List<Result> results = new();

        barrier = new(clientThreads, (b) =>
        {
            Console.WriteLine("All works connected");
        });
        barrier2 = new(clientThreads, (b) =>
        {
            Console.WriteLine("Barrier 2 reached");
        });

        int j = 0;
        for (int i = 0; i < clientThreads; i++)
        {
            var workloadGeneratorCopy = (WorkloadGenerator)workloadGenerator.ShallowCopy();
            workloadGeneratorCopy.ResetKeyLoc();
            Result result = new() { workloadGenerator = workloadGeneratorCopy };
            string address = serverAddresses[j];
            int index = j;
            int pos = i;
            Thread thread = new(() => Worker(workloadGeneratorCopy, in result, address, index, pos, running.Token));

            thread.Name = $"Client {i} for Server {j}";
            threads.Add(thread);
            results.Add(result);
            
            if (j == serverAddresses.Length - 1)
                j = 0;
            else
                j++;
        }

        var startTime = DateTime.Now;
        var endTime = startTime.AddSeconds(duration);


        // start all threads
        foreach (Thread thread in threads)
        {
            thread.Start();
        }

        while (DateTime.Now < endTime)
        {
            Thread.Sleep(500);
            // print elapsed time / duration, in the same line
            if (!supressRunningStatus)
                Console.Write($"\r{DateTime.Now - startTime} / {duration}s");
        }

        Console.WriteLine("Run Complete");

        running.Cancel();

        // wait for all threads to finish
        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        barrier.Dispose();
        barrier2.Dispose();

        return results;
    }


    DateTime lastRecievedMsgTime;
    public void VerifyResults(ClientMessage msg, Result resultTracker)
    {        
        if (msg.result == false)
        {
            Console.WriteLine($"Failed to execute {msg.opCode} on {msg.key} because {msg.response}");
        }  
        else if (msg.key is not null && msg.key.Equals("stats"))
        {
            var stats = msg.response.Split(":")[1].Split(",").Select(x => uint.Parse(x)).ToList();
            resultTracker.throughputs = stats;
        }
        else
        {
            lastRecievedMsgTime = DateTime.Now;

            resultTracker.recvTime.Add((msg, DateTime.Now));
            if (msg.sequenceNumber > resultTracker.last_recv_seq)
                resultTracker.last_recv_seq = msg.sequenceNumber;
        }
    
    }


    private Result Worker(in WorkloadGenerator workloadGenerator, in Result result, string address, int index, int pos, CancellationToken running)
    {
        ServerConnection conn = new(address.Split(":")[0], int.Parse(address.Split(":")[1]));
        conn.Connect();
        conn.StartReceiveResult(VerifyResults, result);
        barrier.SignalAndWait();

        result.startTime = DateTime.Now;

        var lastStatsRequestTime = DateTime.Now;
        uint statSeq = 0;

        int sentThisBatch = 0;

        while (!running.IsCancellationRequested)
        {
            ClientMessage cmd;
            var curTime = DateTime.Now;         
            
            cmd = workloadGenerator.GetNextCommand(index);
            result.sentTime.Add((cmd, curTime));
            result.last_sent_seq = cmd.sequenceNumber;
        
            try
            {
                conn.Transmit(cmd);           
            }
            catch (Exception e)
            {
                Console.WriteLine($"Server connection to {conn.ip}:{conn.port} lost becuase of {e.GetType()} {e.Message} at {e.StackTrace} when sending {cmd} \n Stopping worker, stopped at {(DateTime.Now - result.startTime).Seconds}s");
                break;
            }

            if (perThreadOutputBatch > 0)
            {
                if (sentThisBatch < perThreadOutputBatch)
                    sentThisBatch++;
                else
                {
                    sentThisBatch = 0;
                    Thread.Sleep(threadSleepInterval);
                }
            }
        }

        // Console.WriteLine($"stopped sending to {conn.ip}:{conn.port} at {DateTime.Now}");

        int count = 0;
        // wait for all messages to be received
        while (result.sentTime.Count > result.recvTime.Count &&  (DateTime.Now - lastRecievedMsgTime).TotalMilliseconds < 5000 && count < 100)
        {       
            Thread.Sleep(100);
            count++;
        } 

        if (index == pos)
        {
            var cmd = RequestStats(statSeq);
            conn.Transmit(cmd);
        }
        Thread.Sleep(1000);

        result.endTime = DateTime.Now;  
        result.duration = result.endTime - result.startTime;

        barrier2.SignalAndWait();
        // Console.WriteLine($"{result.sentTime.Count} {result.recvTime.Count} from {conn.ip}:{conn.port}");
        conn.StopReceiveResult();

        return result;
    
    }

    protected WorkloadGenerator GetWorkloadByTypeCode(string typeCode, int numObjs, float[] opsRatio, float safeRatio)
    {
        switch (typeCode)
        {
            case "pnc":
                return new PNCWorkloadGenerator(typeCode, numObjs, opsRatio, safeRatio);
            case "orset":
                return new ORSetWorkloadGenerator(typeCode, numObjs, opsRatio, safeRatio);
            default:
                throw new Exception($"Unknown type code {typeCode}");
        }

    }

    private ClientMessage RequestStats(uint seq)
    {
        return new ClientMessage()
        { 
            sourceType = SourceType.Client,
            sequenceNumber = seq,
            key = "stats", 
            typeCode = "stats", 
            opCode = "", 
            isSafe = false, 
            paramsList = new string[] { } 
        };
    }
}