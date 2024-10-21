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


public class CMNode : IDAGMsgSenderNode
{
    public int nodeid { get; }
    public string ip { get; }
    public int port { get; }
    public bool isSelf { get; }
    
    public TcpClient tcpSession { get; private set; }
    // the network stream for this node
    public NetworkStream stream { get; private set; }

    private Channel<DAGMessage> sendQueue;
    private ChannelReader<DAGMessage> reader;
    private ChannelWriter<DAGMessage> writer;
    private CancellationTokenSource cts;
    private Thread senderThread;

    private ILogger logger;

    /// <summary>
    /// Creating a the local node
    /// </summary>
    /// <param name="nodeid"></param>
    /// <param name="ip"></param>
    /// <param name="port"></param>
    /// <param name="logger"></param>
    public CMNode(int nodeid, string ip, int port, bool isSelf, ILogger logger = null)
    {
        this.nodeid = nodeid;
        this.ip = ip;
        this.port = port;
        this.isSelf = true;
        
        if (!isSelf)
        {
            this.tcpSession = new TcpClient();
            this.sendQueue = Channel.CreateUnbounded<DAGMessage>();
            this.reader = sendQueue.Reader;
            this.writer = sendQueue.Writer;
            this.cts = new CancellationTokenSource();
        }

        this.logger = logger ?? new NullLogger<CMNode>();
    }

    public void SendMsg(DAGMessage message)
    {
        writer.TryWrite(message);
    }

    public void StartSender()
    {
        CancellationToken ct = cts.Token;
        senderThread = new Thread(async () => 
        {
            while (true)
            {
                try
                {
                    // ct is used to break the loop when needed
                    // no need for a flag
                    DAGMessage msg = await reader.ReadAsync(ct);
                    if (msg == null)
                        break;

                    Serializer.SerializeWithLengthPrefix(stream, msg, PrefixStyle.Base128, (int)msg.type);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (IOException e)
                {
                    logger.LogError("Failed to send message to node {nodeid} because of {eMessage}", nodeid, e.Message);
                    return;
                }
            }
        });

        senderThread.Priority = ThreadPriority.Highest;

        senderThread.Start();
    }

    public bool NodeConnect()
    {
        try
        {
            tcpSession.Connect(ip, port);
            stream = tcpSession.GetStream();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    public void NodeDisconnect()
    {
        cts.Cancel();
        senderThread.Join();
        stream.Close();
        tcpSession.Close();
    }
    
}
