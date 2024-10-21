using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Timers;
using MathNet.Numerics.Distributions;

namespace BFTCRDT.Client;

public enum AccessPattern
{
    uniform,
    normal,
}


public class BankingBenchmarkRunner : BenchmarkRunners
{
    private Barrier barrier;
    private Barrier barrier2;

    Random random = new();
    Normal normalDistribution;
    AccessPattern accessPattern;

    public BankingBenchmarkRunner(BenchmarkConfig benchmarkConfig) : base(benchmarkConfig)
    {
        // using safeRatio to determine access pattern 
        // so we don't need to change the config file
        if (benchmarkConfig.safeRatio == 0)
            accessPattern = AccessPattern.uniform;
        else if (benchmarkConfig.safeRatio == 1)
            accessPattern = AccessPattern.normal;
        else
            throw new ArgumentException("Invalid Access Pattern");

    }

    public new void Init()
    {
        base.Init();

        normalDistribution = new(keys.Length / 2.0, keys.Length / 6.0); 
    }


    public BankingBenchmarkResults Run()
    {

        barrier = new(clientThreads, (b) =>
        {
            Console.WriteLine("All works connected");
        });
        barrier2 = new(clientThreads, (b) =>
        {
            Console.WriteLine("All works finished");
        });

        List<Thread> threads = new();
        CancellationTokenSource  running = new CancellationTokenSource ();
        BankingBenchmarkResults result = new();

        int j = 0;
        for (int i = 0; i < clientThreads; i++)
        {
            string address = serverAddresses[j];

            Thread thread = new(() => Worker(in result, address, running.Token));

            thread.Name = $"Client {i} for Server {j}";
            threads.Add(thread);
            
            if (j == serverAddresses.Length - 1)
                j = 0;
            else
                j++;
        }

        // start all threads
        foreach (Thread thread in threads)
        {
            thread.Start();
        }

        var startTime = DateTime.Now;
        var endTime = startTime.AddSeconds(duration);

        while (DateTime.Now < endTime)
        {
            Thread.Sleep(500);
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

        return result;
    }



    private void Worker(in BankingBenchmarkResults result, string address, CancellationToken running)
    {
        ServerConnection conn = new(address.Split(":")[0], int.Parse(address.Split(":")[1]));
        BankingWorkload bankingWorkload = new(conn);
        conn.Connect();
        conn.StartReceiveResult(bankingWorkload.NotifyReceived, null);
        barrier.SignalAndWait(running);
        int sentThisBatch = 0;
        int sentCount = 0;

        Stopwatch sw = new();
        Stopwatch sw2 = new();
        List<float> throughputEachSecond = new();

        sw2.Start();
        while (!running.IsCancellationRequested)
        {
            var curTime = DateTime.Now;         

            int op = workloadGenerator.PickRandomOptionByRatio();       
            string account = GetRandomAccount(); 
        
            try
            {
                sw.Start();
                switch (op)
                {
                    case 0:
                        bankingWorkload.ViewBalance(account);
                        break;
                    case 1:
                        bankingWorkload.Deposit(account, random.Next(1000));
                        break;
                    case 2:
                        // generate 50% chance of withdraw or transfer
                        if (random.Next(2) == 0)
                        {
                            bankingWorkload.Transfer(account, GetRandomAccount(), random.Next(100));
                        }
                        else
                        {
                            bankingWorkload.Withdraw(account, random.Next(100));
                            op = 3;
                        }
                        break;
                    default:
                        throw new ArgumentException("Invalid operation");
                }
                sw.Stop();
                sw.Reset();
                sentCount++;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Server connection to {conn.ip}:{conn.port} lost because of {e.GetType()} {e.Message} at {e.StackTrace}");
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

            var passedTime = sw2.ElapsedMilliseconds;
            if (passedTime > 1000)
            {
                throughputEachSecond.Add((float)(bankingWorkload.intervalRecvCount / (sw2.ElapsedMilliseconds / 1000.0)));
                Interlocked.Exchange(ref bankingWorkload.intervalRecvCount, 0);
                sw2.Restart();
            }

        }
        
        Thread.Sleep(10000);

        barrier2.SignalAndWait(60000);
        conn.StopReceiveResult();

        bankingWorkload.PopulateResults(in result);
        result.throughputs.Add(throughputEachSecond);
    }




    public string GetRandomAccount()
    {
        switch (accessPattern)
        {
            case AccessPattern.uniform:
                // Using uniform distribution
                int randomIndexUniform = random.Next(keys.Length);
                return keys[randomIndexUniform];

            case AccessPattern.normal:
                // Using normal distribution
                int randomIndexNormal = (int)Math.Round(normalDistribution.Sample());
                randomIndexNormal = Math.Max(0, Math.Min(randomIndexNormal, keys.Length - 1)); 
                return keys[randomIndexNormal];

            default:
                throw new ArgumentException("Invalid distribution type");
        }
    }


}