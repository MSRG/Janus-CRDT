using System;
using System.Collections.Generic;
using System.Threading;
using MergeSharp;
using BFTCRDT.DAG;

namespace BFTCRDT;


public interface ISafeCRDTWrapper
{

    public CRDT crdt { get; }

    public object Query(object[] args = null);
    public object Update(int operationID, object[] args = null);
}

public class SafeCRDT
{

    public readonly Guid guid; 
    public readonly string key;
    private ISafeCRDTWrapper stableState;
    private ISafeCRDTWrapper prospectiveState;
    private SafeCRDTManager sm;

    public SafeCRDT(Guid guid, string key, ISafeCRDTWrapper stableState, ISafeCRDTWrapper prospectiveState, SafeCRDTManager sm)
    {
        this.guid = guid;
        this.key = key;
        this.stableState = stableState;
        this.prospectiveState = prospectiveState;
        this.sm = sm;
    }

    

    public object Update(int operationID, object[] args, bool isSafe, (Connection, uint) origin = default)
    {   
        object result;
        NetworkProtocol syncMsg = new NetworkProtocol();

        
        lock (prospectiveState.crdt)
        {
            result = prospectiveState.Update(operationID, args);
            // well this is very hacky    
            
            syncMsg.uid = this.guid;
            syncMsg.syncMsgType = NetworkProtocol.SyncMsgType.CRDTMsg;
            syncMsg.message = prospectiveState.crdt.GetLastSynchronizedUpdate().Encode();
        }

        if (isSafe && origin != default)
            sm.safeUpdateTracker.TryAdd(syncMsg, origin);

        sm.ActualPropagateSyncMsg(syncMsg);
       
        return result;

    }

    public object QueryStable(object[] args = null)
    {
        lock (stableState.crdt)
        {
            return stableState.Query(args);
        }
    }

    public object QueryProspective(object[] args = null)
    {
        lock (prospectiveState.crdt)
        {
            return prospectiveState.Query(args);
        }
    }
    
    public void ApplyUpdateStable(NetworkProtocol msg)
    {
        stableState.crdt.ApplySynchronizedUpdate(stableState.crdt.DecodePropagationMessage(msg.message));
    }

}