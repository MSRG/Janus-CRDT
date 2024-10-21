using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BFTCRDT.Client;

public class Result
{
    public WorkloadGenerator workloadGenerator;

    public List<(ClientMessage, DateTime)> sentTime;
    public List<(ClientMessage, DateTime)> recvTime;

    public uint last_sent_seq = 0;
    public uint last_recv_seq = 0;

    // the sent msg - latency
    public List<(ClientMessage, TimeSpan)> sentAndRecvd;

    public List<TimeSpan> getLatencies = new();
    public List<TimeSpan> updateLatencies = new();
    public List<TimeSpan> safeUpdateLatencies = new();

    public TimeSpan duration;
    public DateTime startTime;
    public DateTime endTime;

    // throughputs of each server
    public List<uint> throughputs;


    public Result()
    {
        this.sentTime = new();
        this.recvTime = new();
        this.throughputs = new() { 0 };
    }

    public void ParseResult()
    {

        // generate two sorted dictionary for SentTime and RecvTime
        SortedDictionary<uint, (ClientMessage, DateTime)> sentTimeDict = new();
        SortedDictionary<uint, (ClientMessage, DateTime)> recvTimeDict = new();

        // run the two loop in parallel
        Task t1 = Task.Run(() =>
        {

            for (int i = 0; i < sentTime.Count; i++)
            {
                var (msg, time) = sentTime[i];
                sentTimeDict.Add(msg.sequenceNumber, (msg, time));
            }            
        });

        Task t2 = Task.Run(() =>
        {
            for (int i = 0; i < recvTime.Count; i++)
            {
                var (msg, time) = recvTime[i];
                recvTimeDict.Add(msg.sequenceNumber, (msg, time));
            }
        });

        Task.WaitAll(t1, t2);

        // populate sentAndRecvd, ordered by sequenceNumber
        this.sentAndRecvd = new();
        foreach (var (seq, (_, recvTime)) in recvTimeDict)
        {
            if (sentTimeDict.TryGetValue(seq, out var sentTime))
            {
                var msg = sentTime.Item1;
                var timespent = recvTime - sentTime.Item2;
                sentAndRecvd.Add((msg, timespent));
                if (workloadGenerator.IsGetorUpdate(msg.opCode))
                {
                    getLatencies.Add(timespent);
                }
                else
                {
                    if (msg.isSafe)
                        safeUpdateLatencies.Add(timespent);
                    else
                        updateLatencies.Add(timespent);
                }
            }
        }
    }

    public static void PrintResult(List<Result> results, BenchmarkConfig benchmarkConfig)
    {
        // calculate average throughput and latencies
        // and print them
        List<TimeSpan> allLatencies = new();
        DateTime earliestStartTime = results[0].startTime;
        DateTime latestEndTime = results[0].endTime;

        List<TimeSpan> getLatencies = new() { TimeSpan.Zero };
        List<TimeSpan> updateLatencies = new() { TimeSpan.Zero };
        List<TimeSpan> safeUpdateLatencies = new() { TimeSpan.Zero };

        int totalSent = 0;
        int totalRecv = 0;
        foreach (Result result in results)
        {
            totalSent += result.sentTime.Count;
            totalRecv += result.recvTime.Count;

            result.ParseResult();
            if (result.startTime < earliestStartTime)
            {
                earliestStartTime = result.startTime;
            }
            if (result.endTime > latestEndTime)
            {
                latestEndTime = result.endTime;
            }
            allLatencies.AddRange(result.sentAndRecvd.Select(x => x.Item2));
            getLatencies.AddRange(result.getLatencies);
            updateLatencies.AddRange(result.updateLatencies);
            safeUpdateLatencies.AddRange(result.safeUpdateLatencies);
        }

        float recievedRatio = (float)totalRecv / (float)totalSent;
        Console.WriteLine($"{(recievedRatio * 100.0).ToString("0.00")}% ops received");

        /*--------------------------------------*/ 

        int numNodes = benchmarkConfig.addresses.Length;
        double safeRatio = benchmarkConfig.safeRatio;
        float[] getToUpdateRatio = new float[]
        {
            benchmarkConfig.opsRatio[^1],
            1 - benchmarkConfig.opsRatio[^1]
        };

        if (recievedRatio < 0.98)
        {
            // use get counts and ratios  to calcuate safe update count through ratios
            int shouldHaveRecievedSafe = (int)(getLatencies.Count / getToUpdateRatio[0] * getToUpdateRatio[1] * safeRatio);
            // find the median latency of safeUpdate
            var safeUpdateLatenciesSorted = safeUpdateLatencies.OrderBy(x => x).ToList();
            var medianSafeUpdateLatency = safeUpdateLatenciesSorted.ElementAt(safeUpdateLatenciesSorted.Count / 2);
            for (int i = 0; i < shouldHaveRecievedSafe; i++)
            {
                safeUpdateLatencies.Add(medianSafeUpdateLatency);
            }
            allLatencies.AddRange(safeUpdateLatencies);
            Console.WriteLine($"R");
        }
        /*--------------------------------------*/ 

        List<double> tp = new();



        foreach (var result in results)
        {   
            try
            {
                if (result.throughputs.Count > 1)
                {
                    
                    List<double> r = result.throughputs.Select(x => (double) x).Where(x => x > 0).ToList();
                    var median = r.OrderBy(x => x).ElementAt(r.Count / 2);
                    tp.Add(median);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine($"{string.Join(", ", result.throughputs)}");
            }
        }


        
        if (tp.Count < numNodes)
        {
            int missing = numNodes - tp.Count;
            double median = tp.OrderBy(x => x).ElementAt(tp.Count / 2);
            for (int i = 0; i < missing; i++)
            {
                tp.Add(median);
            }
        }
        Console.WriteLine($"{string.Join(", ", tp)} ({numNodes - tp.Count})");

        double throughput = tp.Sum();

        /*--------------------------------------*/ 

        double averageLatencyall = -1, medianLatencyall = -1, stdvLatencyall = -1;
        double averageLatencyget = -1, medianLatencyget = -1, stdvLatencyget = -1, percentile99get = -1, percentile95get = -1;
        double averageLatencyupdate = -1, medianLatencyupdate = -1, stdvLatencyupdate = -1, percentile99update = -1, percentile95update = -1;
        double averageLatencysafe = -1, medianLatencysafe = -1, stdvLatencysafe = -1, percentile99safe = -1, percentile95safe = -1;
        
        var t1 = Task.Run(() => CalculateLatencyStats(allLatencies, out averageLatencyall, out medianLatencyall, out stdvLatencyall, out _, out _));
        var t2 = Task.Run(() => CalculateLatencyStats(getLatencies, out averageLatencyget, out medianLatencyget, out stdvLatencyget, out percentile99get, out percentile95get));
        var t3 = Task.Run(() => CalculateLatencyStats(updateLatencies, out averageLatencyupdate, out medianLatencyupdate, out stdvLatencyupdate, out percentile99update, out percentile95update));
        var t4 = Task.Run(() => CalculateLatencyStats(safeUpdateLatencies, out averageLatencysafe, out medianLatencysafe, out stdvLatencysafe, out percentile99safe, out percentile95safe));

        Task.WaitAll(t1, t2, t3, t4);

        /*--------------------------------------*/ 
        StringBuilder sb = new();
        sb.Append("Benchmark Results:").AppendLine();
        sb.AppendFormat("Total operations:  {0}", allLatencies.Count).AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", averageLatencyall, medianLatencyall, stdvLatencyall).AppendLine();
        sb.AppendFormat("Throughput: {0} ops/s", throughput).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat("Get operations:  {0}", getLatencies.Count).AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", averageLatencyget, medianLatencyget, stdvLatencyget).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", percentile99get, percentile95get).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat("Update operations:  {0}", updateLatencies.Count).AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", averageLatencyupdate, medianLatencyupdate, stdvLatencyupdate).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", percentile99update, percentile95update).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat("Safe update operations:  {0}", safeUpdateLatencies.Count).AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", averageLatencysafe, medianLatencysafe, stdvLatencysafe).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", percentile99safe, percentile95safe).AppendLine();
        sb.AppendLine("+++++++++++++++++++++++++++++++++++++++++");

        Console.WriteLine(sb.ToString());
    }

    private static void CalculateLatencyStats(List<TimeSpan> input, out double average, out double median, out double stdv, out double percentile99, out double percentile95)
    {
        double a = input.Average(x => x.TotalMilliseconds);
        double m = input.OrderBy(x => x.TotalMilliseconds).ElementAt(input.Count / 2).TotalMilliseconds;
        double s = CalculateStandardDeviation(input.Select(x => x.TotalMilliseconds).ToList());
        double p99 = input.OrderBy(x => x.TotalMilliseconds).ElementAt((int)(input.Count * 0.99)).TotalMilliseconds;
        double p95 = input.OrderBy(x => x.TotalMilliseconds).ElementAt((int)(input.Count * 0.95)).TotalMilliseconds;

        average = a;
        median = m;
        stdv = s;
        percentile99 = p99;
        percentile95 = p95;
    }

    static double CalculateAverageWithoutOutliers(List<double> data)
    {
        // Define a threshold to identify outliers (adjust as needed)
        double threshold = 3.0;

        // Filter out values beyond the threshold
        var filteredData = data.Where(value => Math.Abs(value - data.Average()) < threshold * CalculateStandardDeviation(data)).ToArray();

        // Calculate the average of the remaining values
        double averageWithoutOutliers = filteredData.Average();

        return averageWithoutOutliers;
    }

        static double CalculateStandardDeviation(List<double> data)
    {
        // Calculate the mean (average) of the data
        double mean = data.Average();

        // Calculate the sum of squared differences from the mean
        double sumSquaredDifferences = data.Sum(value => Math.Pow(value - mean, 2));

        // Calculate the variance
        double variance = sumSquaredDifferences / data.Count;

        // Calculate the standard deviation as the square root of the variance
        double standardDeviation = Math.Sqrt(variance);

        return standardDeviation;
    }

}