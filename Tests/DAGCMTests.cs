using Xunit;
using System;
using BFTCRDT;
using BFTCRDT.DAG;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace Tests;
[Collection("CannotParallel")]
public class DAGCMTests : IDisposable
{   
    List<ILogger> loggers;
    ConfigParser config0, config1, config2, config3;
    Cluster cl0, cl1, cl2, cl3;
    DAG dag0, dag1, dag2, dag3;
    ConnectionManager cm0, cm1, cm2, cm3;
    List<ConnectionManager> cms;

    public DAGCMTests()
    {
        loggers = new();
        for (int i = 0; i < 4; i++)
        {

            var lvl = LogLevel.None;
            if (i == 0)
            {
                lvl = LogLevel.None;
            }
            loggers.Add(LoggerFactory.Create(builder =>
            {
                LogLevel logLevel = lvl;
                builder.AddConsole().SetMinimumLevel(logLevel);
                builder.AddDebug().SetMinimumLevel(logLevel);

            }).CreateLogger("Node" + i));
        }
        

        config0 = new("../../../TestAssets/FourNodes/test_cluster.0.json");
        config1 = new("../../../TestAssets/FourNodes/test_cluster.1.json");
        config2 = new("../../../TestAssets/FourNodes/test_cluster.2.json");
        config3 = new("../../../TestAssets/FourNodes/test_cluster.3.json");


        cl0 = config0.GetCluster(loggers[0]);
        cl1 = config1.GetCluster(loggers[1]);
        cl2 = config2.GetCluster(loggers[2]);
        cl3 = config3.GetCluster(loggers[3]);

        dag0= CreateDAG(cl0, loggers[0]);
        dag1 = CreateDAG(cl1, loggers[1]);
        dag2 = CreateDAG(cl2, loggers[2]);
        dag3 = CreateDAG(cl3, loggers[3]);

        
        cm0 = new(cl0, dag0, loggers[0]);
        cm1 = new(cl1, dag1, loggers[1]);
        cm2 = new(cl2, dag2, loggers[2]);
        cm3 = new(cl3, dag3, loggers[3]);

        cms = new() { cm0, cm1, cm2, cm3 };

        Parallel.ForEach(cms, cm =>
        {
            cm.Start();
        });
   
    }

    [Fact]
    public void TestInit()
    {
        // Test if all of them are init
        foreach (var c in cms)
        {
            Assert.True(c.cluster.isConnected);
        }

        Parallel.ForEach(cms, cm =>
        {
            cm.StartDAG();
        });

        foreach (var cm in cms)
        {
            Assert.True(cm.dag.isInit);
        }
    }

    [Fact]
    public void TestDAGRun()
    {
        TestInit();

        // Sleep for 10 seconds
        System.Threading.Thread.Sleep(10000);

        // Stop
        Parallel.ForEach(cms, cm =>
        {
            cm.Stop();
        });

        List<int> rounds = new();
        foreach (var cm in cms)
        {
            DAG dag = cm.dag;
            rounds.Add(int.Parse(dag.getCurrRoundState()[0]));
        }

        string s = string.Join(",", rounds);

        // check if all are over 50 rounds
        Assert.True(rounds.All(x => x >= 50), $"Not all nodes are over 50 rounds, rounds are: {s}");
    }

    [Fact]
    public void TestConsensus()
    {
        TestDAGRun();
        var orderedBlocksOfNode0 = cms[0].dag.consensus.orderedBlocks;
        foreach (var cm in cms)
        {
            DAG dag = cm.dag;
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;
                for (int i = 0; i < 100; i++)
                {
                    Assert.Equal(orderedBlocksOfNode0[i].blockHash, orderedBlocksOfNodei[i].blockHash, new ByteArrayComparer());
                }
            }
        }

    }

    private DAG CreateDAG(Cluster cluster, ILogger logger)
    {
        List<Replica> replicas = Replica.GenerateReplicas(cluster.nodes.Keys.ToList(), cluster.self.nodeid, out Replica selfReplica);
        DAGMsgSender msgSender = new(cluster.self.nodeid, cluster.GetNodesList(), logger);
        DAG dag = new(selfReplica, replicas, msgSender, logger);

        Consensus consensus = new(dag, logger);
        dag.consensus = consensus;

        return dag;
    }


    public void Dispose()
    {

        Parallel.ForEach(cms, cm =>
        {
            cm.Stop();
            cm.Dispose();
        });
    }
}