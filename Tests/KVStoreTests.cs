using Xunit;
using System;
using BFTCRDT;
using BFTCRDT.DAG;
using System.Collections.Generic;
using System.Threading.Tasks;
using MergeSharp;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Collections.Concurrent;
using Planetarium.Cryptography.BLS12_381;
using ProtoBuf;

namespace Tests;

public class Node
{
    public Cluster cluster;
    public KeySpaceManager ksm;
    public SafeCRDTManager sm;
    public ConnectionManager cm;
    public ReplicationManager rm;
    public DAG dag;
    public ConfigParser config;
    public ILogger logger;

    public Node(string configFile, LogLevel loggingLevel)
    {
        Console.WriteLine("Initializing services");

        this.config = new(configFile);

        logger = LoggerFactory.Create(builder =>
            {
                builder.AddConsole().SetMinimumLevel(loggingLevel);
                builder.AddDebug().SetMinimumLevel(loggingLevel);

            }).CreateLogger("Node" + config.selfNode.nodeid);

        Serializer.PrepareSerializer<ClientMessage>();
        Serializer.PrepareSerializer<InitMessage>();
        Serializer.PrepareSerializer<CertificateMessage>();
        Serializer.PrepareSerializer<VertexBlockMessage>();
        Serializer.PrepareSerializer<SignatureMessage>();
        Serializer.PrepareSerializer<BlockQueryMessage>();

        
    }

    public void Init()
    {
        this.cluster = config.GetCluster(logger);
        
        List<Replica> replicas = Replica.GenerateReplicas(cluster.nodes.Keys.ToList(), cluster.self.nodeid, out Replica selfReplica);
        DAGMsgSender msgSender = new(cluster.self.nodeid, cluster.GetNodesList(), logger);
        this.dag = new(selfReplica, replicas, msgSender, logger);

        Consensus consensus = new(dag, logger);
        dag.consensus = consensus;
        this.cm = new ConnectionManager(cluster, dag, logger);
        this.rm = new ReplicationManager(cm, null, logger);
        this.sm = new(cm, rm, logger)
        {
            clientBatchSize = 1
        };

        cm.StartDAG();
        
        this.ksm = new(config.selfNode.nodeid, config.isPrimary(), sm, rm, logger);
        dag.consensus.consensusCompleteEvent += ksm.safeCRDTManager.HandleAfterConsensusUpdates;
        ksm.InitializeKeySpace();
        
    }

    public void Stop()
    {
        ksm.Stop();
    }

}

[Collection("CannotParallel")]
public class KVStoreTests : IDisposable
{
    
    
    volatile bool safeUpdateComplete = false;

    int numNodes = 4;
    List<Node> nodes = new();

    public KVStoreTests()
    {
        for (int i = 0; i < numNodes; i++)
        {

            var lvl = LogLevel.None;
            if (i == 0)
            {
                lvl = LogLevel.None;
            }

            Node node = new("../../../TestAssets/FourNodes/test_cluster." + i + ".json", lvl);
            nodes.Add(node);

        }
    }

    [Fact]
    public void TestInit()
    {
        // create a list of tasks that contains nodes[i].Init() but do not start them just yet, start them at the same time and wait them to finish
        var tasks = nodes.Select(n => Task.Run(() => n.Init())).ToArray();
        Task.WaitAll(tasks);
        for (int i = 0; i < numNodes; i++)
        {
            Assert.True(nodes[i].ksm.keySpaceInitialized);
            nodes[i].sm.safeUpdateCompleteClientNotifier += NotifySafeUpdateComplete;
        }
    }


    [Fact]
    public void TestCreateCRDT()
    {
        TestInit();

        nodes[0].ksm.CreateNewKVPair<PNCounter>("test");

        System.Threading.Thread.Sleep(200);
        for (int j = 0; j < numNodes; j++)
        {
            nodes[j].ksm.GetKVPair("test", out SafeCRDT value);
            if (value is null)
                Console.WriteLine("Failed to get CRDT from node " + j);
            Assert.NotNull(value);
        }
    }

    [Fact]
    public void TestSimpleIncrement()
    {
        TestCreateCRDT();

        var kms0 = nodes[0].ksm;
        kms0.GetKVPair("test", out SafeCRDT value0);

        value0.Update(1, new object[] { 5 }, false);
        int p0 = (int) value0.QueryProspective();
        
        System.Threading.Thread.Sleep(200);
        foreach (var n in nodes)
        {
            var k = n.ksm;
            k.GetKVPair("test", out SafeCRDT value);
            int p = (int) value.QueryProspective();
            Assert.Equal(p0, p);
        }
    }

    [Fact]
    public void TestCreateMultipleCRDTs()
    {
        TestInit();

        var kms0 = nodes[0].ksm;
        List<SafeCRDT> values = new();

        for (int i = 0; i < 100; i++)
        {
            kms0.CreateNewKVPair<PNCounter>("test" + i);
        }

        System.Threading.Thread.Sleep(500);
        foreach (var n in nodes)
        {
            var k = n.ksm;
            for (int i = 0; i < 100; i++)
            {
                k.GetKVPair("test" + i, out SafeCRDT value);
                Assert.NotNull(value);
            }
        }
    }

    [Fact]
    public void TestMultipleIncrement()
    {
        TestCreateMultipleCRDTs();

        var kms0 = nodes[0].ksm;
        List<SafeCRDT> r0values = new();

        for (int i = 0; i < 100; i++)
        {
            kms0.GetKVPair("test" + i, out SafeCRDT r0value_i);
            r0values.Add(r0value_i);
        }

        for (int i = 0; i < 100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                Random rnd = new();
                int r = rnd.Next(0, 100);
                r0values[i].Update(1, new object[] { r }, false);
            }
        }

        System.Threading.Thread.Sleep(500);

        foreach (var n in nodes)
        {
            var k = n.ksm;
            for (int i = 0; i < 100; i++)
            {
                k.GetKVPair("test" + i, out SafeCRDT rk_value_i);
                int p = (int) rk_value_i.QueryProspective();
                Assert.Equal(r0values[i].QueryProspective(), p);
            }
        }

    }

    [Fact]
    public void TestStableConverge()
    {
        TestCreateCRDT();

        var kms0 = nodes[0].ksm;
        kms0.GetKVPair("test", out SafeCRDT value0);

        value0.Update(1, new object[] { 5 }, false);
        int p0 = (int) value0.QueryProspective();
        
        System.Threading.Thread.Sleep(1000);
        foreach (var n in nodes)
        {
            var k = n.ksm;
            k.GetKVPair("test", out SafeCRDT value);
            int p = (int) value.QueryProspective();
            Assert.Equal(p0, p);
            int s = (int) value.QueryStable();
            Assert.Equal(p, s);
        }
    }

    [Fact]
    public void TestMultipleConverge()
    {        
        TestCreateMultipleCRDTs();

        var kms0 = nodes[0].ksm;
        List<SafeCRDT> r0values = new();

        for (int i = 0; i < 100; i++)
        {
            kms0.GetKVPair("test" + i, out SafeCRDT r0value_i);
            r0values.Add(r0value_i);
        }

        for (int i = 0; i < 100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                Random rnd = new();
                int r = rnd.Next(0, 100);
                r0values[i].Update(1, new object[] { r }, false);
            }
        }

        System.Threading.Thread.Sleep(1000);

        foreach (var n in nodes)
        {
            var k = n.ksm;
            for (int i = 0; i < 100; i++)
            {
                k.GetKVPair("test" + i, out SafeCRDT rk_value_i);
                int p = (int) rk_value_i.QueryProspective();
                Assert.Equal(r0values[i].QueryProspective(), p);
                int s = (int) rk_value_i.QueryStable();
                Assert.Equal(p, s);
            }
        }
    }

    [Fact]
    public void TestSafeUpdate()
    {
        TestCreateCRDT();

        var kms0 = nodes[0].ksm;
        kms0.GetKVPair("test", out SafeCRDT value0);

        value0.Update(1, new object[] { 5 }, false);
        int p = (int) value0.QueryProspective();
        int s = (int) value0.QueryStable();
        // immediately after update, prospective and stable should be different
        Assert.NotEqual(p, s);

        value0.Update(1, new object[] { 3 }, true, (null, 1));
        
        // should be equal since it blocks until consensus is complete
        for (int i = 0; i < 50; i++)
        {
            if (safeUpdateComplete)
                break;
            else if (i == 49 && !safeUpdateComplete)
                Assert.True(false);
            
            System.Threading.Thread.Sleep(200);
        }

        p = (int) value0.QueryProspective();
        s = (int) value0.QueryStable();
        Assert.Equal(p, s);
    }
    
    [Fact]
    public void TestMultipleSafeUpdate()
    {
        TestCreateMultipleCRDTs();

        Random rnd = new();
        uint x = 1;
        foreach (var n in nodes)
        {
            var k = n.ksm;
            for (int i = 0; i < 10; i++)
            {
                k.GetKVPair("test" + i, out SafeCRDT rk_value_i);
                // get random number
                Assert.NotNull(rk_value_i.Update(1, new object[] { rnd.Next(1, 100) }, true, (null, x)));
                x++;

                for (int j = 0; j < 50; j++)
                {
                    if (safeUpdateComplete)
                        break;
                    else if (j == 49 && !safeUpdateComplete)
                        Assert.True(false, "Safe update not complete for test " + i + " at node " + n.dag.self.nodeid);
                    
                    System.Threading.Thread.Sleep(200);
                }

                var p = (int) rk_value_i.QueryProspective();
                var s = (int) rk_value_i.QueryStable();
                // should be equal since it blocks until consensus is complete
                Assert.Equal(p, s);
                safeUpdateComplete = false;
            }
        }
    }

    private void NotifySafeUpdateComplete((Connection, uint) origin)
    {
        safeUpdateComplete = true;
    }

    public void Dispose()
    {
        Parallel.ForEach(nodes, n => n.Stop());
    }
}
