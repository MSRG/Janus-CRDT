using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using BFTCRDT;
using Microsoft.Extensions.Logging;

namespace BFTCRDT.DAG;

public abstract class Block
{
    public int round { get; }
    public int source { get; }



    public Block(int round, int source)
    {
        this.round = round;
        this.source = source;
    }
}


public class VertexBlock : Block
{
    public byte[] digest { get; set; }

    public Certificate certificate { get; set; }
    public List<UpdateMessage> updateMsgs { get; set; }
    public List<byte[]> prevCertificates { get; set; }

    public byte[] signature { get; set; }


    public VertexBlock(int round, int source, List<byte[]> prevCertificates, List<UpdateMessage> updates, byte[] digest = null) : base(round, source)
    {
        this.updateMsgs = updates;
        this.prevCertificates = prevCertificates ?? new List<byte[]>();
        this.updateMsgs = updates ?? new List<UpdateMessage>();
        this.digest = digest ?? ComputeDigest();
    }
    
    public byte[] ComputeDigest()
    {
        byte[] toSign = ArrayPool<byte>.Shared.Rent(sizeof(int) * 1 + sizeof(int) * 1 + prevCertificates.Count * 32 + updateMsgs.Count * 32);
        Array.Clear(toSign, 0, toSign.Length);
        try
        {
            // create to byte array to sign with round, source, prevCertificates's digest, and all updates digest (digest are all 32 bytes)
            Array.Copy(BitConverter.GetBytes(round), 0, toSign, 0, sizeof(int));
            Array.Copy(BitConverter.GetBytes(source), 0, toSign, sizeof(int) * 1, sizeof(int));
            int offset = sizeof(int) * 2;
            foreach (var cert in prevCertificates)
            {
                Array.Copy(cert, 0, toSign, offset, 32);
                offset += 32;
            }
            foreach (var update in updateMsgs)
            {
                Array.Copy(update.digest, 0, toSign, offset, 32);
                offset += 32;
            }

            var result = SHA256.HashData(toSign);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(toSign);
        }
    }

    public void Sign(ECDsa privateKey)
    {
        this.signature = ComputeSignature(privateKey);
    }

    public byte[] ComputeSignature(ECDsa privateKey)
    {
        return privateKey.SignData(this.digest, HashAlgorithmName.SHA256);
    }

    public bool Verify(ECDsa publicKey)
    {
        return publicKey.VerifyData(ComputeDigest(), signature, HashAlgorithmName.SHA256);
    }
}


public class Certificate : Block
{
    public byte[] blockHash { get; set; }
    
    public bool committed { get; set; } = false;

    public Dictionary<int, byte[]> receivedSignatures { get; set; }

    public Certificate(int round, 
    int source, 
    byte[] blockHash, 
    Dictionary<int, byte[]> signatures, // <source, signature>
    byte[] digest = null) : base(round, source)
    {
        this.blockHash = blockHash;
        this.receivedSignatures = signatures;
    }

    public bool  CheckSignatures(Dictionary<int, ECDsa> replicaKeys)
    {
        foreach (var sig in receivedSignatures)
        {
            if (!replicaKeys[sig.Key].VerifyData(blockHash, sig.Value, HashAlgorithmName.SHA256))
                return false;
        }

        return true;

    }

}

