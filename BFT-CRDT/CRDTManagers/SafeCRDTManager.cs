using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BFTCRDT.DAG;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;


public delegate void SafeUpdateCompleteClientNotifier((BFTCRDT.Connection, uint) origin);

public class SafeCRDTManager
{

    public static Dictionary<Type, Type> TypeMap = new() {
        {typeof(PNCounter), typeof(PNCounterWrapper)},
        {typeof(ORSet<string>), typeof(ORSetWrapper)}
    };

    // TODO: duh fix this
    public ConnectionManager cm;
    private ReplicationManager rm;

    public Dictionary<string, SafeCRDT> safeCRDTs { get; private set; }
    public Dictionary<Guid, SafeCRDT> safeCRDTsIndexedByuid { get; private set; }
    public ConcurrentDictionary<NetworkProtocol, (Connection client, uint seqNum)> safeUpdateTracker { get; private set; }
    public SafeUpdateCompleteClientNotifier safeUpdateCompleteClientNotifier { get; set; }

    public int clientBatchSize { get; init; } = 1;

    private ConcurrentQueue<NetworkProtocol> clientUpdateBuffer;

    ILogger logger;

    public SafeCRDTManager(ConnectionManager cm, ReplicationManager rm, ILogger logger = null)
    {
        this.rm = rm;
        this.cm = cm;

        this.safeCRDTs = new();
        this.safeCRDTsIndexedByuid = new();
        this.safeUpdateTracker = new();
        this.clientUpdateBuffer = new();

        this.logger = logger ?? new NullLogger<SafeCRDTManager>();

    }


    /// <summary>
    /// The prospective state is handled by RM, and the stable state is handled b
    /// </summary>
    /// <param name="key"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public SafeCRDT CreateSafeCRDT<T>(string key) where T : CRDT
    {
        // create a wrapper instance of given T
        var guid = rm.CreateCRDTInstance<T>(out T crdtInstance);
        var prospectiveState = Activator.CreateInstance(TypeMap[typeof(T)], crdtInstance);

        // create a instance of T without rm
        var crdtInstanceStable = Activator.CreateInstance(typeof(T));
        var stableState = Activator.CreateInstance(TypeMap[typeof(T)], crdtInstanceStable);

        SafeCRDT sc = new(guid, key, (ISafeCRDTWrapper)stableState, (ISafeCRDTWrapper)prospectiveState, this);
        safeCRDTs[key] = sc;
        safeCRDTsIndexedByuid[guid] = sc;

        return sc;
    }

    /// <summary>
    ///  An prospective state that is already created by RM in this case. Used for synchronizing a CRDT creation
    /// from another replica.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="guid"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public SafeCRDT CreateSafeCRDT(string key, Guid guid)
    {
        Type t = rm.GetCRDTType(guid).BaseType;

        // create a instance of T without rm
        var crdtInstanceStable = Activator.CreateInstance(t);
        var stableState = Activator.CreateInstance(TypeMap[t], crdtInstanceStable);
        rm.TryGetCRDT(guid, out CRDT crdtInstance);
        var prospectiveState = Activator.CreateInstance(TypeMap[t], crdtInstance);

        SafeCRDT sc = new(guid, key, (ISafeCRDTWrapper)stableState, (ISafeCRDTWrapper)prospectiveState, this);
        safeCRDTs[key] = sc;
        safeCRDTsIndexedByuid[guid] = sc;

        return sc;
    }


    /// <summary>
    /// Actions when DAG complete a consensus round
    /// </summary>
    /// <param name="vbs"></param>
    SemaphoreSlim semaphore = new SemaphoreSlim(1, 1); // TODO: semaphore does not guarantee FIFO, need to fix this
    public void HandleAfterConsensusUpdates(List<List<UpdateMessage>> updates)
    {
        if (updates is null)
            return;

        // run it in a task to avoid blocking the DAG
        Task thisTask = Task.Run(() =>
        {   
            semaphore.Wait();

            try
            {
                // apply updates to the stable state
                foreach (var list in updates)
                {
                    foreach (var block in list)
                    {
                        foreach (var u in block.update)
                        {

                            //logger.LogDebug("Consensused update msg: uid {0} type {1} ", u.uid, u.syncMsgType);

                            Guid uid = u.uid;
                            // the creation of a instance is already done when the block is first received
                            if (u.syncMsgType == NetworkProtocol.SyncMsgType.ManagerMsg_Create || uid.CompareTo(Guid.Empty) == 0)
                                continue;

                            if (safeCRDTsIndexedByuid.TryGetValue(uid, out SafeCRDT sc))
                            {
                                // check for invarient if needed
                                sc.ApplyUpdateStable(u);

                                if (safeUpdateTracker.TryRemove(u, out (BFTCRDT.Connection, uint) origin) && safeUpdateCompleteClientNotifier is not null)
                                    safeUpdateCompleteClientNotifier(origin);
                            }
                        }
                    }
                }

                // reverse the perspective state
                

            }
            finally
            {
                semaphore.Release();
            }

        });


    }



    public DateTime lastSumittedTime = DateTime.Now;
    public void ActualPropagateSyncMsg(NetworkProtocol msg)
    {

        clientUpdateBuffer.Enqueue(msg);

        if (clientUpdateBuffer.Count >= clientBatchSize || (DateTime.Now - lastSumittedTime).TotalMilliseconds > 100)
        {
            Dictionary<Guid, NetworkProtocol> appearedObjects = new();

            List<NetworkProtocol> msgs = new(clientBatchSize);
            while (clientUpdateBuffer.TryDequeue(out NetworkProtocol np) && msgs.Count < clientBatchSize)
            {
                // if the update is a not a safe update
                if (!safeUpdateTracker.ContainsKey(np))
                    appearedObjects[np.uid] = np;
                else // if it is a safe update, add it to the message list individually becuase it needs to be notified later
                    msgs.Add(np);
            }

            // since state based CRDT, only needs to send the latest update of each object
            // order doesn't matter here - we are in the same batch
            foreach (var item in appearedObjects)
                msgs.Add(item.Value);


            if (msgs.Count > 0)
            {
                UpdateMessage umsg = new(msgs);
                cm.dag.SubmitMessage(umsg);
                lastSumittedTime = DateTime.Now;
            }

        }
    }
}



