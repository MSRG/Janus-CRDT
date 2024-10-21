using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;

namespace BFTCRDT.DAG;


/// <summary>
/// Class responsible for managing the committee and keeping track of the status of the committee
/// </summary>
public class Committee
{
    // <nodeid: replica>
    public Dictionary<int, Replica> nodes { get; }
    public Dictionary<int, int> atRounds { get; }
    public Dictionary<int, ECDsa> nodeKeys { get; private set; }
    public int numNodes { get; }

    public Committee(List<Replica> nodes)
    {
        this.nodes = new();
        this.atRounds = new();

        foreach (var r in nodes)
        {
            this.nodes.Add(r.nodeid, r);
            this.atRounds.Add(r.nodeid, 0);
        }

        this.numNodes = nodes.Count;
    }  

    public Replica GetNode(int nodeid)
    {
        return nodes[nodeid];
    }

    public ECDsa GetNodeKey(int nodeid)
    {
        return nodeKeys[nodeid];
    }

    public int[] NodeIds()
    {
        return nodes.Keys.ToArray();
    }
    
    public bool IsInitd()
    {
        bool f = nodes.Values.Any(n => n.publicKeyHandler == null);
        if (!f)
        {
            this.nodeKeys = nodes.ToDictionary(node => node.Value.nodeid, node => node.Value.publicKeyHandler);
        }
        return !f;
    }


}