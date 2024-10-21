using Xunit;
using PBFTConnectionManager;
using System;
using Microsoft.Extensions.Logging;

namespace Tests;

// adding this to prevent it from running parallel with the
// cluster tests
[Collection("collection1")]
public class ConsensusTests : IDisposable
{

    public Cluster node1Cluster, node2Cluster, node3Cluster, node4Cluster;
    public ManagerServer node1Server, node2Server, node3Server, node4Server;

    public ConsensusTests()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            LogLevel logLevel;

            logLevel = LogLevel.Debug;
  
            builder.AddConsole().SetMinimumLevel(logLevel);
            builder.AddConsole();

        });

        var node1logger = loggerFactory.CreateLogger("Node1");
        var node2logger = loggerFactory.CreateLogger("Node2");
        var node3logger = loggerFactory.CreateLogger("Node3");
        var node4logger = loggerFactory.CreateLogger("Node4");


        node1Cluster = new Cluster(new string[] {
            "127.0.0.1:8000",
            "127.0.0.1:8001",
            "127.0.0.1:8002",
            "127.0.0.1:8003"}
            , "127.0.0.1", 8000);

        node2Cluster = new Cluster(new string[] {
            "127.0.0.1:8000",
            "127.0.0.1:8001",
            "127.0.0.1:8002",
            "127.0.0.1:8003"}
            , "127.0.0.1", 8001);

        node3Cluster = new Cluster(new string[] {
            "127.0.0.1:8000",
            "127.0.0.1:8001",
            "127.0.0.1:8002",
            "127.0.0.1:8003"}
            , "127.0.0.1", 8002);

        node4Cluster = new Cluster(new string[] {
            "127.0.0.1:8000",
            "127.0.0.1:8001",
            "127.0.0.1:8002",
            "127.0.0.1:8003"}
            , "127.0.0.1", 8003);


        node1Server = new ManagerServer(node1Cluster.selfNode, node1Cluster);
        node2Server = new ManagerServer(node2Cluster.selfNode, node2Cluster);
        node3Server = new ManagerServer(node3Cluster.selfNode, node3Cluster);
        node4Server = new ManagerServer(node4Cluster.selfNode, node4Cluster);

        node1Server.Run();
        node2Server.Run();
        node3Server.Run();
        node4Server.Run();
        
        
    }

    [Fact]
    public void ConsensusTestWorking()
    {
        node1Cluster.InitCluster();
        node2Cluster.InitCluster();
        node3Cluster.InitCluster();
        node4Cluster.InitCluster();

        // sleep for 5 seconds
        System.Threading.Thread.Sleep(5000);

        Assert.True(node1Cluster.isAllAlive);
        Assert.True(node2Cluster.isAllAlive);
        Assert.True(node3Cluster.isAllAlive);
        Assert.True(node4Cluster.isAllAlive);

        Assert.True(node1Server.sender.NewPBFTRequest("test"));
    }

    [Fact]
    public void ConsensusTestOneFailure()
    {
        node1Cluster.InitCluster();
        node2Cluster.InitCluster();
        node3Cluster.InitCluster();
        node4Cluster.InitCluster();

        // sleep for 5 seconds
        System.Threading.Thread.Sleep(5000);

        Assert.True(node1Cluster.isAllAlive);
        Assert.True(node2Cluster.isAllAlive);
        Assert.True(node3Cluster.isAllAlive);
        Assert.True(node4Cluster.isAllAlive);

        node4Server.Stop();

        Assert.True(node1Server.sender.NewPBFTRequest("test"));
    }


    [Fact]
    public void ConsensusTestRedirectRequestToPrimary()
    {
        node1Cluster.InitCluster();
        node2Cluster.InitCluster();
        node3Cluster.InitCluster();
        node4Cluster.InitCluster();

        // sleep for 5 seconds
        System.Threading.Thread.Sleep(5000);

        Assert.True(node1Cluster.isAllAlive);
        Assert.True(node2Cluster.isAllAlive);
        Assert.True(node3Cluster.isAllAlive);
        Assert.True(node4Cluster.isAllAlive);

        Assert.True(node2Server.sender.NewPBFTRequest("test1"));
        Assert.True(node3Server.sender.NewPBFTRequest("test2"));
    }


    public void Dispose()
    {
        node1Server.Stop();
        node2Server.Stop();
        node3Server.Stop();
        node4Server.Stop();
    }
}