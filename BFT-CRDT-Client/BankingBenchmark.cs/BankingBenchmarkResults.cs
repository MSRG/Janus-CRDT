using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MathNet.Numerics.Statistics;

namespace BFTCRDT.Client;

public class BankingBenchmarkResults
{
    
    public int totalView = 0;
    public int totalDeposit = 0;
    public int totalWithdraw = 0;
    public int totalTransfer = 0;

    public ConcurrentBag<List<(int, TimeSpan)>> latencyResults = new();
    public ConcurrentBag<List<float>> throughputs = new();


    public void Parse()
    {
        // parse latency
        List<TimeSpan> viewLatencies = new() {TimeSpan.Zero};
        List<TimeSpan> depositLatencies = new() {TimeSpan.Zero};
        List<TimeSpan> transferLatencies = new() {TimeSpan.Zero};
        List<TimeSpan> withdrawLatencies = new() {TimeSpan.Zero};


        foreach (var result in latencyResults)
        {
            foreach (var (opCode, latency) in result)
            {
                switch (opCode)
                {
                    case 0:
                        viewLatencies.Add(latency);
                        break;
                    case 1:
                        depositLatencies.Add(latency);
                        break;
                    case 2:
                        transferLatencies.Add(latency);
                        break;
                    case 3:
                        withdrawLatencies.Add(latency);
                        break;
                    
                }
            }
        }

        CalculateLatencyStats(viewLatencies, out double viewAverage, out double viewMedian, out double viewStdv, out double viewPercentile99, out double viewPercentile95);
        CalculateLatencyStats(depositLatencies, out double depositAverage, out double depositMedian, out double depositStdv, out double depositPercentile99, out double depositPercentile95);
        CalculateLatencyStats(transferLatencies, out double transferAverage, out double transferMedian, out double transferStdv, out double transferPercentile99, out double transferPercentile95);
        CalculateLatencyStats(withdrawLatencies, out double withdrawAverage, out double withdrawMedian, out double withdrawStdv, out double withdrawPercentile99, out double withdrawPercentile95);

        // parse throughput, fine the sum of median
        List<float> medTps = new();
        foreach (var tp in throughputs)
        {
            List<float> r = tp.Select(x => (float) x).Where(x => x > 0).ToList();
            var median = r.OrderBy(x => x).ElementAt(r.Count / 2);
            medTps.Add((float) median);
        }

        Console.WriteLine($"{string.Join(", ", medTps)}");
        float totaltp = medTps.Sum();

        StringBuilder sb = new();
        sb.Append("Benchmark Results:").AppendLine();

        sb.AppendFormat($"Total throughput: {totaltp}").AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat($"View Ops: {totalView} succeed {viewLatencies.Count}").AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", viewAverage, viewMedian, viewStdv).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", viewPercentile99, viewPercentile95).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat($"Deposit Ops: {totalDeposit} succeed {depositLatencies.Count}").AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", depositAverage, depositMedian, depositStdv).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", depositPercentile99, depositPercentile95).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat($"Transfer Ops: {totalTransfer} succeed {transferLatencies.Count}").AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", transferAverage, transferMedian, transferStdv).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", transferPercentile99, transferPercentile95).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendFormat($"Withdraw Ops: {totalWithdraw} succeed {withdrawLatencies.Count}").AppendLine();
        sb.AppendFormat("Average latency:  {0} ms; Median latency: {1} ms; Stdv latency: {2} ms", withdrawAverage, withdrawMedian, withdrawStdv).AppendLine();
        sb.AppendFormat("99th percentile latency: {0} ms; 95th percentile latency: {1} ms", withdrawPercentile99, withdrawPercentile95).AppendLine();
        sb.AppendLine("----------------------------------------");
        sb.AppendLine("+++++++++++++++++++++++++++++++++++++++++");

        
        Console.WriteLine(sb.ToString());
    }

    private static void CalculateLatencyStats(List<TimeSpan> input, out double average, out double median, out double stdv, out double percentile99, out double percentile95)
    {
        // remove all timespan with max value
        input.RemoveAll(x => x == TimeSpan.MaxValue);
        average = input.Average(x => x.TotalMilliseconds);
        median = input.OrderBy(x => x).ElementAt(input.Count / 2).TotalMilliseconds;
        stdv = input.Select(x => x.TotalMilliseconds).StandardDeviation();
        percentile99 = input.Select(x => x.TotalMilliseconds).Percentile(99);
        percentile95 = input.Select(x => x.TotalMilliseconds).Percentile(95);
    }

}