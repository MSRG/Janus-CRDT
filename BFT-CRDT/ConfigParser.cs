
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using BFTCRDT.DAG;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;

public struct Node
{
    [JsonInclude]
    public int nodeid;
    [JsonInclude]
    public string address;
    [JsonInclude]
    public int port;
    [JsonInclude]
    public bool isSelf;

}

public class ConfigParser
{
    public List<Node> nodes { get; private set; }

    public Node selfNode { get; private set; }

    public ConfigParser(string filename)
    {
        this.nodes = new List<Node>();
        ParseConfigJson(filename);
    }


    private void ParseConfigJson(string filename)
    {
        this.nodes = JsonSerializer.Deserialize<List<Node>>(File.ReadAllText(filename));

        // sanity check
        // check if multiple selves
        int selfNodeCount = 0;
        // check if duplicate nodes
        HashSet<string> addrportSet = new HashSet<string>();

        foreach (var n in nodes)
        {
            if (n.isSelf)
            {
                this.selfNode = n;
                selfNodeCount++;
            }

            if (selfNodeCount > 1)
            {
                throw new Exception("Config: Too many self node!");
            }

            // check for port validity
            if (n.port < 0 || n.port > 62535)
            {
                throw new Exception($"Config: Invalid port number {n.port}");
            }

            string addrport = n.address + n.port.ToString();
            if (addrportSet.Contains(addrport))
                throw new Exception("Duplicate nodes!");
            else
                addrportSet.Add(addrport);
        }

        if (selfNodeCount == 0)
        {
            throw new Exception("Config: No self node");
        }

        StringBuilder listingNodes = new StringBuilder("The following nodes are assigned to the cluster:\n");
        foreach (var n in nodes)
            listingNodes.AppendLine(n.nodeid + " - " + n.address + ":" + n.port.ToString());

        Console.WriteLine(listingNodes.ToString());
        
        // print self node info
        Console.WriteLine("Self node: " + selfNode.nodeid + " - " + selfNode.address + ":" + selfNode.port.ToString());
    }

    public bool isPrimary()
    {
        int smallest = int.MaxValue;
        foreach (var node in nodes)
        {
            if (node.nodeid < smallest)
            {
                smallest = node.nodeid;
            }
        }

        return selfNode.nodeid == smallest;
    }


    public Cluster GetCluster(ILogger logger)
    {
        List<CMNode> clusterNodes = new();
        CMNode selfNode = null;

        foreach (var n in nodes)
        {
            CMNode node = new(n.nodeid, n.address, n.port, n.isSelf, logger);
            if (n.isSelf)
                selfNode = node;
            clusterNodes.Add(node);
        }

        Debug.Assert(selfNode != null);

        Cluster cluster = new(selfNode, clusterNodes, logger);
        return cluster;
    }
}
