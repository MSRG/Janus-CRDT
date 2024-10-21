using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace BFTCRDT;

public class PerfCounter
{
    public uint lastsecOps = 1;
    public ulong totalops = 1;

    public List<uint> samples;
    
    private ILogger logger;

    private volatile bool reportRunning;



    public PerfCounter(ILogger logger)
    {
        this.logger = logger;
        samples = new();
    }

    public void OpAdd(ulong size)
    {
        if (reportRunning)
        {
            Interlocked.Increment(ref lastsecOps);
            Interlocked.Increment(ref totalops);
            //Interlocked.Add(ref totalSizes, size);
        }
    }
    
    public void OpSubtract(ulong size)
    {
        if (reportRunning)
        {
            Interlocked.Decrement(ref lastsecOps);
            Interlocked.Decrement(ref totalops);
        }
    }

    public void StartReport()
    {
        reportRunning = true;
        var t = new Thread(() =>
        {
            Stopwatch sw = new();
            sw.Start();
            Thread.Sleep(1000);
            while (reportRunning)
            {   
                sw.Stop();
                float actualtp = lastsecOps / ((float)sw.Elapsed.TotalMilliseconds / 1000.0f);

                //logger.LogDebug("PerfCounter: {0} {1} ", lastsecOps, sw.Elapsed.TotalMilliseconds);

                samples.Add((uint)actualtp);
                Interlocked.Exchange(ref lastsecOps, 1);

                sw.Reset();
                sw.Start();
                Thread.Sleep(1000);
            }
        });
        t.Start();
    }


    public List<uint> GetReport(out ulong totalops)
    {
        totalops = this.totalops;
        return samples;

    }

    public void StopReport()
    {
        reportRunning = false;
    }
}