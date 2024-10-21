using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Security.Cryptography;
using ProtoBuf;
using MergeSharp;
using System.Buffers;

namespace BFTCRDT.DAG;


public enum MessageType : byte
{
    // Need to start at 1 for protobuf resolver
    padding = 1,
    VertexBlockMessage,
    CertificateMessage,
    SignatureMessage,
    BlockQueryMessage,
    InitMessage,
    PlainCRDTMessage
}

public static class MessageTypeResolver
{
    public static Dictionary<MessageType, Type> TypeMap = new Dictionary<MessageType, Type>()
    {
        { MessageType.VertexBlockMessage, typeof(VertexBlockMessage) },
        { MessageType.CertificateMessage, typeof(CertificateMessage) },
        { MessageType.SignatureMessage, typeof(SignatureMessage) },
        { MessageType.BlockQueryMessage, typeof(BlockQueryMessage) },
        { MessageType.InitMessage, typeof(InitMessage) },
        { MessageType.PlainCRDTMessage, typeof(PlainCRDTMessage) }
    };

    public static Type ResolveType(int fieldNumber)
    {
        if (!TypeMap.TryGetValue((MessageType)fieldNumber, out Type type))
            throw new Exception("Invalid field number");
        return type;
    }

    public static int ResolveFieldNumber<T>() where T : DAGMessage
    {
        return (byte)Enum.Parse(typeof(MessageType), typeof(T).Name);
    }
}

[ProtoContract]
[ProtoInclude(100, typeof(VertexBlockMessage))]
[ProtoInclude(200, typeof(CertificateMessage))]
[ProtoInclude(300, typeof(SignatureMessage))]
[ProtoInclude(400, typeof(BlockQueryMessage))]
[ProtoInclude(500, typeof(InitMessage))]
[ProtoInclude(600, typeof(PlainCRDTMessage))]
public abstract class DAGMessage
{
    public MessageType type { get; protected set; }
    [ProtoMember(1), DefaultValue(-1)]
    public int round { get; protected set; } = -1;
    [ProtoMember(2), DefaultValue(-1)]
    public int source { get; protected set; } = -1;
}



[ProtoContract]
public class VertexBlockMessage : DAGMessage
{
    [ProtoMember(1)]
    public byte[] digest { get; }
    [ProtoMember(2)]
    public byte[] signature { get; }
    [ProtoMember(3)]
    public List<byte[]> prevCertificates { get; }
    [ProtoMember(4, IsPacked = true, DataFormat = DataFormat.Group)]
    public List<UpdateMessage> updates { get; }
    // this is for the query reply, to reuse the same message type
    [ProtoMember(5)]
    public bool IsQueryReply { get; } = false;

    public VertexBlockMessage() { }

    public VertexBlockMessage(VertexBlock block, bool IsQueryReply = false)
    {
        this.type = MessageType.VertexBlockMessage;

        this.round = block.round;
        this.source = block.source;
        this.digest = block.digest;
        this.signature = block.signature;
        this.prevCertificates = block.prevCertificates;
        this.updates = block.updateMsgs;
        this.IsQueryReply = IsQueryReply;
    }

    public VertexBlock GetBlock()
    {
        VertexBlock block = new(
            round,
            source,
            prevCertificates,
            updates,
            digest
        );

        block.signature = signature;

        return block;

    }

}

[ProtoContract]
public class CertificateMessage : DAGMessage
{
    [ProtoMember(1)]
    public byte[] blockHash { get; }
    [ProtoMember(2)]
    public Dictionary<int, byte[]> receivedSignatures { get; }

    public CertificateMessage() { }

    public CertificateMessage(Certificate block)
    {
        this.type = MessageType.CertificateMessage;
        
        this.round = block.round;
        this.source = block.source;
        this.blockHash = block.blockHash;
        this.receivedSignatures = block.receivedSignatures;
    }
    public Certificate GetCertificate()
    {
        return new Certificate(
            round,
            source,
            blockHash,
            receivedSignatures
        );
    }

}

[ProtoContract]
public class SignatureMessage : DAGMessage
{
    // [ProtoMember(1)]
    // public byte[] blockHash { get; }
    [ProtoMember(1)]
    public byte[] signature { get; }

    public SignatureMessage() { }

    public SignatureMessage(int source, int round, byte[] signature)
    {
        this.type = MessageType.SignatureMessage;
        this.source = source;
        this.round = round;
        //this.blockHash = blockHash;
        this.signature = signature;
    }

    public bool Verify(byte[] blockHash, ECDsa publicKey)
    {
        return publicKey.VerifyData(blockHash, signature, HashAlgorithmName.SHA256);

    }

}

[ProtoContract]
public class BlockQueryMessage : DAGMessage
{   
    [ProtoMember(1)]
    public byte[] blockHash { get; }
    public BlockQueryMessage() { }

    public BlockQueryMessage(byte[] blockHash, int source)
    {
        this.round = -100; // making sure it is fine
        this.type = MessageType.BlockQueryMessage;
        this.blockHash = blockHash;
        this.source = source;
    }
}

[ProtoContract]
public class InitMessage : DAGMessage
{
    [ProtoMember(1)]
    public byte[] publicKey { get; }
    public InitMessage() {}

    public InitMessage(int source, byte[] publicKey)
    {
        this.round = -100; // making sure it is fine
        this.type = MessageType.InitMessage;
        this.source = source;
        this.publicKey = publicKey;
    }
}

[ProtoContract]
public class PlainCRDTMessage : DAGMessage
{
     [ProtoMember(1, IsPacked = true, DataFormat = DataFormat.Group)]
    public List<NetworkProtocol> msgs;

    public PlainCRDTMessage() { }

    public PlainCRDTMessage(List<NetworkProtocol> msgs)
    {
        this.type = MessageType.PlainCRDTMessage;
        this.source = 1;
        this.round = 1;
        this.msgs = msgs;
    }
}