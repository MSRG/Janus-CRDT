using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using MergeSharp;
using Microsoft.Extensions.Logging;
using BFTCRDT;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT.DAG;

public delegate void ConsensusMessageHandler(DAGMessage msg);

public class ConnectionManager : IConnectionManager
{
    public Cluster cluster { get; private set; }
    private ManagerServer server;
    public DAG dag { get; private set; }

    private ILogger logger;

    public event EventHandler<SyncMsgEventArgs> ReplicationManagerSyncMsgHandlerEvent;
    
    public ConnectionManager(Cluster cluster, DAG dag, ILogger logger = null)
    {
        this.logger =  logger ?? new NullLogger<ConnectionManager>();

        this.cluster = cluster;
        this.dag = dag;

        this.server = new(cluster.self, dag.HandleMessage, this.logger);

        dag.receivedBlockEvent += ReceivedBlock;
    }

    /// <summary>
    /// Actions when DAG received a block
    /// </summary>
    /// <param name="vb"></param>
    public void ReceivedBlock(VertexBlock vb)
    {
        foreach (var u in vb.updateMsgs)
        {
            foreach (var np in u.update)
            {
                //logger.LogDebug("Received update msg: uid {0} type {1} ", np.uid, np.syncMsgType);
                ReplicationManagerSyncMsgHandlerEvent(this, new SyncMsgEventArgs(np));
            }
        }
    }

    public void PropagateSyncMsg(NetworkProtocol msg)
    {
        // let the object creation through
        if (msg.syncMsgType == NetworkProtocol.SyncMsgType.ManagerMsg_Create ||
            (msg.syncMsgType == NetworkProtocol.SyncMsgType.CRDTMsg && Guid.Empty.CompareTo(msg.uid) == 0 ))
        {
            List<NetworkProtocol> msgs = new(1) { msg };
            UpdateMessage umsg = new(msgs);
            dag.SubmitMessage(umsg);
        }
    }

    public void Start()
    {
        server.Run();

        try 
        {
            cluster.ConnectAll();
        }
        catch (Exception)
        {
            server.Stop();
            throw;
        }
    }

    public void StartDAG()
    {
        if (!cluster.isConnected)
            throw new Exception("Cluster is not connected");

        dag.DAGClusterInit();

        // check for dag.isInit for 10 seconds, internal is 1 second, if not throw error
        for (int i = 0; i < 10; i++)
        {
            if (dag.isInit)
                break;
            System.Threading.Thread.Sleep(1000);
        }

        if (!dag.isInit)
            throw new Exception("DAG is not init");
       
        dag.Start();
    }

    public void Stop()
    {
        dag.Stop();
    }

    public void Dispose()
    {
        server.Stop();
    }

    public string[] GetDagStats()
    {
        return dag.GetStats().GetCurrStats();
    }
}
