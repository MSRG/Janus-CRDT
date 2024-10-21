using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BFTCRDT.DAG;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;

namespace BFTCRDT;

public class Connection
{
    public TcpClient client { get; set; }
    public Thread recvThread { get; set; }
    public Thread sendThread { get; set; }
    public Channel<ClientMessage> sendQueue { get; set; }
    public volatile bool active;

    private CancellationTokenSource cts;

    private readonly ILogger logger;

    public Connection(ILogger logger = null)
    {
        cts = new CancellationTokenSource();
        active = true;

        this.logger = logger ?? new NullLogger<Connection>();
    }

    public void StartSender()
    {
        this.sendQueue = Channel.CreateUnbounded<ClientMessage>();
        
        var ct = cts.Token;
        var stream = client.GetStream();

        sendThread = new Thread(async () => 
        {
            var reader = sendQueue.Reader;

            while (true)
            {
                try
                {
                    ClientMessage msg = await reader.ReadAsync(ct);
                    if (msg == null)
                        break;

                    Serializer.SerializeWithLengthPrefix(stream, msg, PrefixStyle.Base128);
                    // logger.LogDebug("Sending message to {0}: {1}", client.Client.RemoteEndPoint, msg);
                    Globals.perfCounter.OpAdd(1);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (IOException)
                {
                    logger.LogInformation($"Client at disconnected, stopping client response sender thread");
                    active = false;
                    sendQueue.Writer.TryComplete();
                    return;
                }
            }
        });

        sendThread.Priority = ThreadPriority.Highest;
        sendThread.Start();
        
    }

    public void Stop()
    {
        logger.LogInformation($"Stopping client response sender thread");
        active = false;
        cts.Cancel();
        client.Close();
        sendQueue.Writer.TryComplete();
    }

    public void Send(ClientMessage msg)
    {
        if (active)
        {
            if (!sendQueue.Writer.TryWrite(msg))
                throw new Exception("Failed to write to send queue");
        }

    }

}


public class ClientInterface
{
    // list of running connections 
    private Dictionary<TcpClient, Connection> connections;
    private TcpListener clientInterfaceServer;
    private CancellationTokenSource cts;

    private KeySpaceManager ksm;

    private string ip;
    private int port;

    private readonly ILogger logger;
    

    private volatile bool running;


    public ClientInterface(string ip, int port , KeySpaceManager ksm,  ILogger logger = null)
    {
        this.connections = new();
        this.ip = ip;
        this.port = port;
        this.ksm = ksm;
        this.ksm.safeCRDTManager.safeUpdateCompleteClientNotifier = NotifySafeUpdateComplete;

        this.logger = logger ?? new NullLogger<ClientInterface>();
    }

    public void Run()
    {

        cts = new CancellationTokenSource();
        CancellationToken ct = cts.Token;

        this.clientInterfaceServer = new TcpListener(IPAddress.Parse(ip), port);
        this.clientInterfaceServer.Start();

        logger.LogInformation("Client interface server started, listening on port {0}", port);

        Thread serverRunThread = new(async () =>
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                
                while (running)
                {
                    var client = await clientInterfaceServer.AcceptTcpClientAsync(ct);

                    logger.LogInformation("New connection from {0}", client.Client.RemoteEndPoint);

                    var t = AcceptNewClient(client, ct);
                    
                    var conn = new Connection(logger) 
                    { 
                        client = client, 
                        recvThread = t
                    };
                    connections.Add(client, conn);
                    conn.StartSender();
                    t.Start();
                    
                    
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Client interface stopped");
                string[] stats = ksm.GetDagStats();
                // print stats
                logger.LogDebug("DAG stats: {0}", string.Join(", ", stats));
            }
            catch (Exception e)
            {
                logger.LogError("Encounter error when accepting new clients {trace}", e.StackTrace);
                throw;
            }

        });

        running = true;
        serverRunThread.Start();
    }

    public void NotifySafeUpdateComplete((Connection, uint) origin)
    {
        var responseMsg = CreateResponse(origin.Item2, true, null);
        origin.Item1.Send(responseMsg);   
    }

    public Thread AcceptNewClient(TcpClient client, CancellationToken ct)
    {
        // start a new thread to handle the client
        Thread t = new Thread(() =>
        {
            try
            {
                NetworkStream ns = client.GetStream();
                while (true)
                {
                    ClientMessage msg = Serializer.DeserializeWithLengthPrefix<ClientMessage>(ns, PrefixStyle.Base128);

                    if (msg is null)
                    {
                        client.Close();
                        logger.LogInformation($"Client disconnected, stopping listerner thread");
                        return;
                    }


                    // logger.LogDebug("Received message from {0}: {1}", client.Client.RemoteEndPoint, msg);

                    // if (msg.key.Equals("STOP"))
                    // {
                    //     logger.LogInformation("Recieved STOP from client, disconnecting");
                    //     throw new OperationCanceledException();
                    // }

                    var cmd = ParseCommand(msg);
                    if (cmd is null)
                    {
                        logger.LogWarning("Failed to parse command from msg {0}", msg);
                        continue;
                    }

                    var origin = (connections[client], msg.sequenceNumber);
                    var res = cmd.Execute(ksm, origin ,out string response);

                    if (!res)
                        logger.LogWarning("Failed to execute msg {0} because {1}", msg, response);

                    bool ifReply = false;
                    // immediately reply if execution failed or is not a safe update
                    if (!string.Equals(response, "su") || !res)
                        ifReply = true;

                    if (ifReply)
                    {
                        var responseMsg = CreateResponse(msg, res, response);
                        connections[client].Send(responseMsg);
                    }

                    // if canceled, break out the loop
                    if (ct.IsCancellationRequested) 
                    {
                        ct.ThrowIfCancellationRequested();
                    }
                }
            }
            catch (IOException)
            {
                logger.LogInformation($"Client at disconnected (IOException), stopping listerner thread.");
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation($"Stopping listerner thread for Client at {client.Client.RemoteEndPoint}");
            }
            catch (Exception e)
            {
                logger.LogError("Encounter error when hanlding client requests at {stacktrace}", e.StackTrace);
                throw;

            }
            

        });

        t.Name = $"Node {client.Client.RemoteEndPoint} handler thread";

        return t;
    }

    public void Stop()
    {
        cts.Cancel();
        running = false;
        clientInterfaceServer.Stop();
        cts.Dispose();
        foreach (var c in connections.Values)
        {
            c.Stop();
            c.recvThread.Join();
        }
    }

    private CRDTCommand ParseCommand(ClientMessage msg)
    {
        if (msg.sourceType != SourceType.Client)
        {
            logger.LogWarning("Received a message from a non-client source on the client interface");
            return null;
        }
        
        var key = msg.key;
        var typeCode = msg.typeCode;
        var opCode = msg.opCode;
        var isSafe = msg.isSafe;
        var paramsList = msg.paramsList;
        
        return CommandFactory.CreateCommand(key, typeCode, opCode, isSafe, paramsList);
    }

    private ClientMessage CreateResponse(ClientMessage sourceMsg, bool result, string response)
    {
        sourceMsg.sourceType = SourceType.Server;
        sourceMsg.paramsList = null;
        sourceMsg.result = result;
        sourceMsg.response = response;

        return sourceMsg;
    }

    
    private ClientMessage CreateResponse(uint seq, bool result, string response)
    {
        return new ClientMessage()
        {
            sourceType = SourceType.Server,
            sequenceNumber = seq,
            result = result,
            response = response,
            isSafe = true
        };
    }

}



