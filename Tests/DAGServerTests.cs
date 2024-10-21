using Xunit;
using System;
using Microsoft.Extensions.Logging;
using BFTCRDT.DAG;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;


namespace Tests;
[Collection("CannotParallel")]
public class DAGServerTests : IDisposable
{

    List<ILogger> loggers;
    List<List<Replica>> replicas;
    List<List<CMNode>> nodes;
    List<ManagerServer> servers;
    List<DAG> dags;
    List<IDAGMsgSender> senders;
    Cluster cluster;
    int numNodes = 4;

    public DAGServerTests()
    {
        loggers = new();
        nodes = new();
        replicas = new();
        servers = new();
        dags = new();
        senders = new();

        string ip = "127.0.0.1";
        int port = 35000;


        // create nodes
        for (int i = 0; i < numNodes; i++)
        {
            nodes.Add(new List<CMNode>());
            replicas.Add(new List<Replica>());

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



            for (int j = 0; j < numNodes; j++)
            {
                CMNode node = new CMNode(j, ip, port + j, j == i, loggers[i]);
                nodes[i].Add(node);

                Replica replica = new(j, j == i);
                replicas[i].Add(replica);
            }

            List<IDAGMsgSenderNode> nodesForSender = new();
            foreach (var n in nodes[i])
                nodesForSender.Add(n);

            IDAGMsgSender msgSender = new DAGMsgSender(i, nodesForSender, loggers[i]);
            senders.Add(msgSender);
        }

        for (int i = 0; i < numNodes; i++)
        {
            DAG dag = new(replicas[i][i], replicas[i], senders[i], loggers[i]);
            dags.Add(dag);
            Consensus consensus = new(dag, loggers[i]);
            dag.consensus = consensus;

            ManagerServer server = new(nodes[i][i], dag.HandleMessage, loggers[i]);
            servers.Add(server);
        }
    }

    [Fact]
    public void TestInit()
    {
        for (int i = 0; i < numNodes; i++)
            servers[i].Run();

        for (int i = 0; i < numNodes; i++)
        {
            for (int j = 0; j < numNodes; j++)
            {
                if (i == j)
                    continue;
                nodes[i][j].NodeConnect();
                nodes[i][j].StartSender();
            }
        }
            

        for (int i = 0; i < numNodes; i++)
            dags[i].DAGClusterInit();

        Thread.Sleep(1000);

        foreach (var dag in dags)
        {
            Assert.True(dag.isInit);
        }
    }

    [Fact]
    public void TestRun()
    {
        List<Task> tasks = new();
        var tokenSource2 = new CancellationTokenSource();
        CancellationToken ct = tokenSource2.Token;

        TestInit();

        loggers[0].LogCritical("Starting tasks");

        foreach (var dag in dags)
        {
            Assert.True(dag.isInit);
            dag.Start();
        }
        // Stop all tasks
        

        int t = 0;
        while (t < 10)
        {
            Thread.Sleep(1000);
            t++;
        }

        loggers[0].LogCritical("Stopping tasks");

        List<int> rounds = new();
        foreach (var dag in dags)
        {
            rounds.Add(int.Parse(dag.getCurrRoundState()[0]));
        }

        string s = string.Join(",", rounds);

        // check if all are over 50 rounds
        Assert.True(rounds.All(x => x >= 50), $"Not all nodes are over 50 rounds, rounds are: {s}");

        Console.WriteLine("Rounds are: " + s);
    }

    [Fact]
    public void TestConsensus()
    {
        TestRun();
        var orderedBlocksOfNode0 = dags[0].consensus.orderedBlocks;

        foreach (var dag in dags)
        {
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;

                for (int i = 0; i < 100; i++)
                {
                    Assert.Equal(orderedBlocksOfNode0[i].blockHash, orderedBlocksOfNodei[i].blockHash, new BFTCRDT.ByteArrayComparer());
                }
            }
        }
    }



    public void Dispose()
    {
        Console.WriteLine("Test finished, stopping servers and dags");

        foreach (var d in dags)
            d.Stop();

        foreach (var s in servers)
            s.Stop();

        foreach (var n in nodes)
        {
            foreach (var node in n)
            {
                if (!node.isSelf)
                    node.NodeDisconnect();
            }
        }

    }
}