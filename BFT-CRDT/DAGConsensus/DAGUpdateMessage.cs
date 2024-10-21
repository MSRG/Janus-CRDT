using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Security.Cryptography;
using ProtoBuf;
using MergeSharp;
using System.Buffers;

namespace BFTCRDT.DAG;

/// <summary>
/// Represent a single update that to be agreed upon by the consensus
/// </summary>
[ProtoContract]
public class UpdateMessage
{   
    [ProtoMember(1)]
    public byte[] digest { get; set; } 
    // This a list because it could be a batch of update
    [ProtoMember(2, IsPacked = true, DataFormat = DataFormat.Group)]
    public List<NetworkProtocol> update;
    public UpdateMessage() { }

    public UpdateMessage(List<NetworkProtocol> update)
    {
        this.update = update;

        this.digest = ComputeDigest();
    }

    public byte[] ComputeDigest()
    {
        // create to byte array to sign with round, source, prevCertificates's digest, and all updates digest (digest are all 32 bytess)
        byte[] toSign = ArrayPool<byte>.Shared.Rent(update.Count * 32);
        Array.Clear(toSign, 0, toSign.Length);
        try
        {
            int offset = 0;
            foreach (var u in update)
            {
                if (u.message is not null)
                    Array.Copy(SHA256.HashData(u.message), 0, toSign, offset, 32);

                offset += 32;
            }

            return SHA256.HashData(toSign);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(toSign);
        }
        
    }

}