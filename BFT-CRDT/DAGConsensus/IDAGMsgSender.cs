using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BFTCRDT.DAG;

public interface IDAGMsgSender 
{
    public void BroadcastBlock(VertexBlock block);

    public void BroadcastCertificate(Certificate certificate);

    public void QueryForBlock(byte[] blockhash, int[] receivers);

    public void SendBlock(VertexBlock block, int receiver, bool IsQueryReply = false);

    public void SendSignature(int round, byte[] signature, int receiver);

    public void BroadCastInit(int source, byte[] publicKey);

}

