
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT.DAG;

/// <summary>
/// Manage a cluster of CMNodes.
/// </summary>
public class Cluster
{
    public CMNode self { get; private set; }
    public Dictionary<int, CMNode> nodes { get; private set; }
    //public NodeMsgSender sender { get; private set; }
    public int numNodes { get => nodes.Count; }

    public bool isConnected { get; private set; } = false;

    private ILogger logger;

    public Cluster(CMNode self, List<CMNode> nodes, ILogger logger = null)
    {
        this.nodes = new();
        this.logger = logger ?? new NullLogger<Cluster>();
        this.self = self;
        this.nodes = nodes.ToDictionary(n => n.nodeid, n => n);
    }

    public List<IDAGMsgSenderNode> GetNodesList()
    {
        return nodes.Values.ToList<IDAGMsgSenderNode>();
    }

    public void ConnectAll()
    {
        // try connect for 5 times, if failed, throw exception
        foreach (var node in nodes.Values)
        {
            if (node.nodeid == self.nodeid)
                continue;

            for (int i = 0; i < 5; i++)
            {
                if (node.NodeConnect())
                    break;
                if (i == 4)
                    throw new Exception("Cannot connect to node " + node.nodeid);
                Thread.Sleep(1000);
            }

            node.StartSender();
        }

        isConnected = true;
    }

    public void DisconnectAll()
    {
        foreach (var node in nodes.Values)
        {
            if (node.nodeid == self.nodeid)
                continue;
            node.NodeDisconnect();
        }
    }
}



