using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;
using System.Threading;
using System.IO;

namespace BFTCRDT.DAG;


public class DAGMsgSender : IDAGMsgSender
{
    private int selfid;
    public Dictionary<int, IDAGMsgSenderNode> cluster { get; private set; }

    private ILogger logger;

    // lock for each node used when msg sender is used by multiple threads
    public static Dictionary<int, object> nodeLockers = new();

    public DAGMsgSender(int selfid, List<IDAGMsgSenderNode> nodes, ILogger logger = null)
    {
        this.selfid = selfid;
        cluster = new Dictionary<int, IDAGMsgSenderNode>();
        foreach (var node in nodes)
        {
            cluster.Add(node.nodeid, node);
            lock (nodeLockers)
            {
                nodeLockers.TryAdd(node.nodeid, new object());
            }
        }

        this.logger = logger ?? new NullLogger<DAGMsgSender>();

    }

    public void BroadcastBlock(VertexBlock block)
    {
        VertexBlockMessage msg = new(block);
        BroadCastMessage(msg);
    }

    public void BroadcastCertificate(Certificate certificate)
    {
        CertificateMessage msg = new(certificate);
        BroadCastMessage(msg);
    }

    public void QueryForBlock(byte[] blockhash, int[] receivers)
    {
        BlockQueryMessage msg = new BlockQueryMessage(blockhash, selfid);
        SendMsg(msg, receivers);
    }

    public void SendBlock(VertexBlock block, int receiver, bool IsQueryReply = false)
    {
        VertexBlockMessage msg = new(block, IsQueryReply);
        SendMsg(msg, receiver);
    }

    public void SendSignature(int round, byte[] signature, int receiver)
    {
        SignatureMessage msg = new(selfid, round, signature);
        SendMsg(msg, receiver);
    }

    public void BroadCastInit(int source, byte[] publicKey)
    {
        InitMessage msg = new(source, publicKey);
        BroadCastMessage(msg);
    }


    public void BroadCastMessage(DAGMessage msg)
    {
        foreach (var node in cluster.Values)
        {
            if (node.nodeid != selfid)
                SendMsg(msg, node.nodeid);
        }
    }

    public void SendMsg(DAGMessage msg, int[] receivers) 
    {

        foreach (var nodeid in receivers)
        {
            if (nodeid != selfid)
                SendMsg(msg, nodeid);
        }
    }


    public void SendMsg(DAGMessage msg, int receiver)
    {
        IDAGMsgSenderNode node = cluster[receiver];

        if (msg.round == -1 || msg.source == -1)
        {
            throw new InvalidOperationException();
        }

        logger.LogTrace($"Sending message of type {msg.GetType()} with round {msg.round} and source {msg.source} to node {receiver}");

        node.SendMsg(msg);
    }
}