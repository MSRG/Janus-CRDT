using Xunit;
using System;
using BFTCRDT;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;

namespace Tests;





[Collection("collection1")]
public class CommandTestsTwoNodes : IDisposable
{
    ConfigFile config0, config1;
    CRDTServices node0, node1;

    public CommandTestsTwoNodes()
    {
        config0 = new("../../../TestAssets/TwoNodes/test_cluster.0.json", "-1");
        config1 = new("../../../TestAssets/TwoNodes/test_cluster.1.json", "-1");

        node0 = new(config0);
        node1 = new(config1);

        //init two nodes in parallel using Parallel.Invoke
        var t1 = Task.Run(() => node0.Init(config0));
        var t2 = Task.Run(() => node1.Init(config1));
        // wait for both to finish
        Task.WaitAll(t1, t2);
    }

    [Fact]
    public void TestTwoNodesSimplePNC()
    {


        var cmd = CommandFactory.CreateCommand("key", "pnc", "s", new string[] { });

        Assert.True(cmd.Execute(node0.ksm, out _));
        // wait 1 second
        Thread.Sleep(1000);
        // get key from node1
        cmd = CommandFactory.CreateCommand("key", "pnc", "g", new string[] { });

        Assert.True(cmd.Execute(node1.ksm, out string output));
        Assert.Equal("0", output);
    }

    [Fact]
    public void TestTwoNodePNCIncrementAndDecrement()
    {
        var cmd = CommandFactory.CreateCommand("key", "pnc", "s", new string[] { });
        cmd.Execute(node0.ksm, out _);

        Thread.Sleep(1000);
        // increment 10 times on both nodes in Parallel.Invoke
        
        var t0 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node0.ksm, out _));
            }
        });

        var t1 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node1.ksm, out _));
            }
        });

        Task.WaitAll(t0, t1);
        
        Thread.Sleep(1000);
        // get key from node1
        cmd = CommandFactory.CreateCommand("key", "pnc", "g", new string[] { });

        Assert.True(cmd.Execute(node0.ksm, out string output));
        Assert.True(cmd.Execute(node1.ksm, out string output1));

        // compare outputs in int
        Assert.Equal(int.Parse(output), int.Parse(output1));


    }

    public void Dispose()
    {
        node0.Stop();
        node1.Stop();
    }
}

[Collection("collection1")]
public class CommandTestsFourNodes : IDisposable
{
    ConfigFile config0, config1, config2, config3;
    CRDTServices node0, node1, node2, node3;

    public CommandTestsFourNodes()
    {
        config0 = new("../../../TestAssets/FourNodes/test_cluster.0.json", "-1");
        config1 = new("../../../TestAssets/FourNodes/test_cluster.1.json", "-1");
        config2 = new("../../../TestAssets/FourNodes/test_cluster.2.json", "-1");
        config3 = new("../../../TestAssets/FourNodes/test_cluster.3.json", "-1");

        node0 = new(config0);
        node1 = new(config1);
        node2 = new(config2);
        node3 = new(config3);

        //init two nodes in parallel using Task.Run
        var t1 = Task.Run(() => node0.Init(config0));
        var t2 = Task.Run(() => node1.Init(config1));
        var t3 = Task.Run(() => node2.Init(config2));
        var t4 = Task.Run(() => node3.Init(config3));
        // wait for both to finish
        Task.WaitAll(t1, t2, t3, t4);
    }

    public void Dispose()
    {
        node0.Stop();
        node1.Stop();
        node2.Stop();
        node3.Stop();
    }

    [Fact]
    public void TestFourNodePNCIncrementAndDecrement()
    {
        var cmd = CommandFactory.CreateCommand("key", "pnc", "s", new string[] { });
        cmd.Execute(node0.ksm, out _);

        Thread.Sleep(1000);
        // increment 10 times on both nodes in parallel
        var t0 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node0.ksm, out _));
            }
        });

        var t1 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node1.ksm, out _));
            }
        });

        var t2 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node2.ksm, out _));
            }
        });

        var t3 = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
            {
                // random int
                var rand = new Random();
                cmd = CommandFactory.CreateCommand("key", "pnc", "i", new string[] { rand.Next(0, 100).ToString() });
                Assert.True(cmd.Execute(node3.ksm, out _));
            }
        });

        Task.WaitAll(t0, t1, t2, t3);

        Thread.Sleep(1000);

        cmd = CommandFactory.CreateCommand("key", "pnc", "g", new string[] { });

        Assert.True(cmd.Execute(node0.ksm, out string output));
        Assert.True(cmd.Execute(node1.ksm, out string output1));
        Assert.True(cmd.Execute(node2.ksm, out string output2));
        Assert.True(cmd.Execute(node3.ksm, out string output3));

        // compare outputs in int
        Assert.Equal(int.Parse(output), int.Parse(output1));
        Assert.Equal(int.Parse(output), int.Parse(output2));
        Assert.Equal(int.Parse(output), int.Parse(output3));
    }
}