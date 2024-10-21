using Xunit;
using System;


using PBFTConnectionManager;
using System.Collections.Generic;
using System.Threading;
using MergeSharp;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Tests;

// xunits classes are automatically run in parallel
[Collection("collection1")]
public class PBFTConnectionManagerMultiNodesTests : IDisposable
{
    ReplicationManager rm0, rm1, rm2, rm3;

    public PBFTConnectionManagerMultiNodesTests()
    {
        var nodeslist = new string[]
        {
                "127.0.0.1:8000",
                "127.0.0.1:8001",
                "127.0.0.1:8002",
                "127.0.0.1:8003"
        };

        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            LogLevel logLevel;

            logLevel = LogLevel.Debug;
  
            builder.AddConsole().SetMinimumLevel(logLevel);
            builder.AddConsole();

        });

        // create a array of 5 loggers
        // var loggers = new ILogger[4];
        // for (int i = 0; i < 4; i++)
        // {
        //     loggers[i] = loggerFactory.CreateLogger("Node" + i);
        // }


        ConnectionManager cm0 = new ConnectionManager("127.0.0.1", 8000, nodeslist);
        ConnectionManager cm1 = new ConnectionManager("127.0.0.1", 8001, nodeslist);
        ConnectionManager cm2 = new ConnectionManager("127.0.0.1", 8002, nodeslist);
        ConnectionManager cm3 = new ConnectionManager("127.0.0.1", 8003, nodeslist);
        
        // create replication managers in parallel
        Parallel.Invoke(
            () => rm0 = new ReplicationManager(cm0),
            () => rm1 = new ReplicationManager(cm1),
            () => rm2 = new ReplicationManager(cm2),
            () => rm3 = new ReplicationManager(cm3)
        );

        // register PNCounter type on each replication manager
        this.rm0.RegisterType<PNCounter>();
        this.rm1.RegisterType<PNCounter>();
        this.rm2.RegisterType<PNCounter>();
        this.rm3.RegisterType<PNCounter>();


    }

    [Fact]
    public void MNTCPClusterPNCTest()
    {
        Guid uid = this.rm0.CreateCRDTInstance<PNCounter>(out PNCounter pnc);

        Thread.Sleep(1000);

        // get the PNCounter from the other nodes
        var pnc1 = this.rm1.GetCRDT<PNCounter>(uid);
        var pnc2 = this.rm2.GetCRDT<PNCounter>(uid);
        var pnc3 = this.rm3.GetCRDT<PNCounter>(uid);

        // list of pncs
        List<PNCounter> pncs = new List<PNCounter>() { pnc, pnc1, pnc2, pnc3 };


        // Increment each PNCounter by a random amount, repeat 5 times
        Random rnd = new Random();
        foreach (var p in pncs)
        {
            int amount = rnd.Next(1, 10);
            p.Increment(amount);
        }

        // check if the same pncounters are replicated on all nodes


        // sleep for 1 second
        Thread.Sleep(5000);

        Assert.Equal(pncs[0].Get(), pncs[1].Get());
        Assert.Equal(pncs[0].Get(), pncs[2].Get());
        Assert.Equal(pncs[0].Get(), pncs[3].Get());

    }



    public void Dispose()
    {
        Thread.Sleep(1000);
    }
}