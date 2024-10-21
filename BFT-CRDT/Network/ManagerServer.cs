using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using ProtoBuf;
using System.Net;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using MergeSharp;

namespace BFTCRDT.DAG;

struct Connection
{
    public TcpClient client;
    public Thread thread;
}


public class ManagerServer
{
    private CMNode selfNode;
    private ConsensusMessageHandler consensusMsgHandler;

    // list of running connections 
    private List<Connection> connections;
    private TcpListener listener;
    private CancellationTokenSource cts;

    private readonly ILogger logger;
    private volatile bool running;

    public ManagerServer(CMNode selfNode, ConsensusMessageHandler consensusMsgHandler,  ILogger logger = null)
    {
        this.connections = new();
        this.selfNode = selfNode;
        this.consensusMsgHandler = consensusMsgHandler;
        this.logger = logger ?? new NullLogger<ManagerServer>();
    }

    public void Run()
    {

        cts = new CancellationTokenSource();
        CancellationToken ct = cts.Token;
        ct.Register(() => { logger.LogInformation("Stopping server"); } );

        this.listener = new TcpListener(IPAddress.Parse("0.0.0.0"), selfNode.port);
        this.listener.Start();

        logger.LogInformation("Manager server started, listening on port {0}", selfNode.port);

        // use task to accept new clients
        Thread serverRunThread = new(async () =>
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                Thread.CurrentThread.Name = $"Node {selfNode.nodeid} server thread";

                while (running)
                {
                    var client = await listener.AcceptTcpClientAsync(ct);

                    logger.LogTrace("New connection from {0}", client.Client.RemoteEndPoint);
                    
                    var t = HandleReceived(client, ct);
                    connections.Add(new Connection() { client = client, thread = t });
                    t.Start();
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Server Listener Stopped");
            }


        });

        running = true;
        serverRunThread.Start();
    }

    public Thread HandleReceived(TcpClient client, CancellationToken ct)
    {
        NetworkStream ns = client.GetStream();

        // start a new thread to handle the client
        Thread t = new Thread(() =>
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                // read the random number from the client in a loop
                object msg;
                while (Serializer.NonGeneric.TryDeserializeWithLengthPrefix(ns, PrefixStyle.Base128, MessageTypeResolver.ResolveType, out msg))
                {

                    if (msg is DAGMessage)
                        consensusMsgHandler((DAGMessage) msg);


                    // if canceled, break out the loop
                    if (ct.IsCancellationRequested) 
                    {
                        client.Close();
                        ct.ThrowIfCancellationRequested();
                    }
                }
            }
            catch (IOException)
            {
                client.Close();
                logger.LogInformation("Cluster member disconnected.");
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Stopping cluster message receiver thread");
            }
            catch (ObjectDisposedException)
            {
                logger.LogInformation("Cluster message receiver already disposed");
            }
            catch (Exception e)
            {
                logger.LogError("Error in cluster message receiver thread: {emsg}", e.Message);
            }
            

        });

        t.Name = $"Node {selfNode.nodeid} handler thread";

        return t;
    }

    public void Stop()
    {   
        if (!running)
            return;
            
        cts.Cancel();
        running = false;
        listener.Stop();
        cts.Dispose();
        foreach (var c in connections)
        {
            c.client.Close();
            c.thread.Join();
        }
    }

    public void ClusterInitHandler()
    {
        
    }

}