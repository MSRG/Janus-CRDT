using System;

namespace BFTCRDT.DAG;


/// <summary>
/// This interface is used for dependency inversion of the network layer and the DAG.
/// The network layer would handle the details of sending and receiving Message objects.
/// </summary>
public interface IDAGMsgSenderNode
{
    public int nodeid { get; }

    public void SendMsg(DAGMessage message);
}
