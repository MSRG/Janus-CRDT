#define NODAG
#undef NODAG

using System;
using System.Collections.Generic;
using System.Linq;
using MergeSharp;
using BFTCRDT.DAG;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using System.Dynamic;

namespace BFTCRDT;

public class KeySpaceManager : IObserver<ReceivedSyncUpdateInfo>
{
    public int nodeid { get; private set; }

    public ConcurrentDictionary<string, Guid> keySpaces;
    public bool keySpaceInitialized = false;
 
    //public Dictionary<string, OpHistoryLog> OpHistoryLogs;
    public SafeCRDTManager safeCRDTManager { get; private set; }

// #if NODAG
//     public TCPConnectionManager.ConnectionManager cm { get; private set; }
// #else
//     public ConnectionManager cm { get; private set; }
// #endif


    private ReplicationManager rm;
    private TPSet<string> replicatedKeySet;
    private IDisposable unsubscriber;

    // if current is the primary node, i.e it has the smallest node id, 
    // then it is responsible for creating the key space set.
    private bool isPrimary;

    private ILogger logger;

    public KeySpaceManager(int nodeid, bool isPrimary, SafeCRDTManager sm, ReplicationManager rm, ILogger logger = null)
    {   
        this.nodeid = nodeid;
        this.isPrimary = isPrimary;
        this.rm = rm;
        this.safeCRDTManager = sm;

        this.logger = logger ?? new NullLogger<KeySpaceManager>();
        
        this.keySpaces = new ConcurrentDictionary<string, Guid>();
    }

    public void InitializeKeySpace()
    {
        
       
// #if NODAG
//         this.cm = new(config) { clientBatchSize = 500 };
// #else
//         this.cm = new(config, 100, 50) { clientBatchSize = 1500 };
// #endif
    
        
        rm.RegisterType<TPSet<string>>();

        this.unsubscriber = rm.Subscribe(this);


// #if NODAG
// #else
//         cm.sm = safeCRDTManager; //TODO: fix this
//         cm.StartDAG();
// #endif

        // get all nodeids from cluster
        // List<int> nodeids = config.cluster.nodes.Select(n => n.nodeid).ToList();
        
        // if self node is the smallest, it is responsible for creating the key space set
        if (isPrimary)
        {
            // waiting for other nodes to detect that cluster is live
            System.Threading.Thread.Sleep(2000);

            // create a new key space as TPset where GUID is always 0
            var KS_uid = rm.CreateCRDTInstance<TPSet<string>>(out replicatedKeySet, Guid.Empty);
            logger.LogTrace("Created new key space set as a 2P-Set with uid: {0}", KS_uid);
        }
        else
        {
            int count = 0;
            while(!rm.TryGetCRDT<TPSet<string>>(Guid.Empty, out this.replicatedKeySet))
            {
                if (count > 5)
                {
                    logger.LogError("Failed to get key space set from master node");
                    throw new TimeoutException("Failed to get key space set from master node");
                }
                System.Threading.Thread.Sleep(1000);
                count++;

            }
            logger.LogTrace("Got key space set from master node");
        }

        // register types
        rm.RegisterType<PNCounter>();
        rm.RegisterType<ORSet<string>>();

        logger.LogInformation("Key Space Set Initialized");    
        keySpaceInitialized = true;
    }

    /// <summary>
    /// Create a new key value pair in the key space set.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="key"></param> <summary>
    // TODO: does not work concurrently
    public void CreateNewKVPair<T>(string key) where T : CRDT
    {
        // 
        // Guid guid = rm.CreateCRDTInstance<T> (out T _);

        SafeCRDT sc = safeCRDTManager.CreateSafeCRDT<T> (key);
        Guid guid = sc.guid;

        // construct a string as "key \0 guid" 
        keySpaces.TryAdd(key, guid);
        replicatedKeySet.Add(key + "\0" + guid.ToString());

        // // create a new op history log for this key
        // var rcode = GetRCRDTCode.Get(typeof(T));
        logger.LogTrace("Added new key {0} with GUID {1}", key, guid);
    }

    public bool GetKVPair(string key, out SafeCRDT value)
    {
        Guid guid = keySpaces[key];
        return safeCRDTManager.safeCRDTs.TryGetValue(key, out value);
    }


    private List<string> LastKeySpaceSet = new List<string>(); 

    /// <summary>
    /// Handler for receiving a sync update from the replication manager
    /// </summary>
    /// <param name="value"></param>
    public void OnNext(ReceivedSyncUpdateInfo value)
    {
        // only handles update for the key space set
        // assume creation of a CRDT does not need consensus
        if (value.guid == Guid.Empty)   
        {
            var newSet = replicatedKeySet.LookupAll();
            // compare the difference between new and old keyspace set
            var diff = newSet.Except(LastKeySpaceSet);
            foreach (var item in diff)
            {
                // if the key is not in the key space set, then it is a new key
                string key = item.Split("\0")[0];
                if (!keySpaces.ContainsKey(key))
                {
                    Guid guid = Guid.Parse(item.Split("\0")[1]);
                    // add new key to key space set
                    keySpaces.TryAdd(key, guid);
                    logger.LogTrace("New key {0} with GUID {1} sync'd", key, guid);

                    safeCRDTManager.CreateSafeCRDT(key, guid);
                }
            }

            LastKeySpaceSet = newSet;
        }
    }


    public string[] GetDagStats()
    {
        return safeCRDTManager.cm.GetDagStats();

    }

    public void OnCompleted()
    {
        unsubscriber.Dispose();
    }

    public void OnError(Exception error)
    {
        logger.LogError(error, "Error when handling sync'd keys");
        throw error;
    }

    public void Stop()
    {
        unsubscriber.Dispose();
        rm.Dispose();
    }
}
