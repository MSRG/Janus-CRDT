using Xunit;
using PBFTConnectionManager;
using System;

namespace Tests;

[Collection("collection1")]
public class ClusterTests : IDisposable
{
    public Cluster node1Cluster, node2Cluster, node3Cluster, node4Cluster;
    public ManagerServer node1Server, node2Server, node3Server, node4Server;

    public ClusterTests()
    {
        node1Cluster = new Cluster(new string[] {
            "127.0.0.1:5000",
            "127.0.0.1:5001",
            "127.0.0.1:5002",
            "127.0.0.1:5003"}
            , "127.0.0.1", 5000);

        node2Cluster = new Cluster(new string[] {
            "127.0.0.1:5000",
            "127.0.0.1:5001",
            "127.0.0.1:5002",
            "127.0.0.1:5003"}
            , "127.0.0.1", 5001);

        node3Cluster = new Cluster(new string[] {
            "127.0.0.1:5000",
            "127.0.0.1:5001",
            "127.0.0.1:5002",
            "127.0.0.1:5003"}
            , "127.0.0.1", 5002);

        node4Cluster = new Cluster(new string[] {
            "127.0.0.1:5000",
            "127.0.0.1:5001",
            "127.0.0.1:5002",
            "127.0.0.1:5003"}
            , "127.0.0.1", 5003);


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
    public void TestClusterInit()
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

        // check if public keys match
        Assert.Equal(node1Cluster.nodes[0].publicKey, node2Cluster.nodes[0].publicKey);
        Assert.Equal(node1Cluster.nodes[0].publicKey, node3Cluster.nodes[0].publicKey);
        Assert.Equal(node1Cluster.nodes[0].publicKey, node4Cluster.nodes[0].publicKey);

    }


    public void Dispose()
    {
        node1Server.Stop();
        node2Server.Stop();
        node3Server.Stop();
        node4Server.Stop();

    }
}
