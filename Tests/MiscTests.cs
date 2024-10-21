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

namespace Tests;

[Collection("CanParallel2")]
public class MiscTests : IDisposable
{
    [Fact]
    public void TestConfigParser()
    {
        var config = new ConfigParser("../../../TestAssets/FourNodes/test_cluster.0.json");
        Assert.Equal(4, config.nodes.Count);
        Assert.Equal(0, config.selfNode.nodeid);
        Assert.True(config.isPrimary());

        var config1 = new ConfigParser("../../../TestAssets/FourNodes/test_cluster.1.json");
        Assert.False(config1.isPrimary());
        
    }

    public void Dispose()
    {
    }

}