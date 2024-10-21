using Xunit;
using BFTCRDT.DAG;
using ProtoBuf;
using System.IO;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using MergeSharp;

namespace Tests;

[Collection("CanParallel1")]
public class DAGBlockAndMsgTests : IDisposable
{
    public ProtoBuf.TypeResolver resolver;
    // random hashes for testing
    public List<byte[]> hashes;

    ECDsa privateKey = ECDsa.Create();
    ECDsa publickey = ECDsa.Create();
    

    public DAGBlockAndMsgTests()
    {
        // setup
        resolver = MessageTypeResolver.ResolveType;
        publickey.ImportSubjectPublicKeyInfo(privateKey.ExportSubjectPublicKeyInfo(), out _); 

        // create a list of 10 32 byte hashes
        hashes = new List<byte[]>();
        for (int i = 0; i < 10; i++)
        {
            byte[] hash = new byte[32];
            new Random().NextBytes(hash);
            hashes.Add(hash);
        }

        
    }

    [Fact]
    public void TestVertexBlockMessage()
    {
        List<UpdateMessage> updates = new List<UpdateMessage>();

        // create 10 CRDT network protocol messages
        for (int i = 0; i < 10; i++)
        {
            NetworkProtocol np = new() { uid = new Guid(), syncMsgType = NetworkProtocol.SyncMsgType.CRDTMsg, type = "test", message = hashes[i] };
            UpdateMessage update = new UpdateMessage(new List<NetworkProtocol>() {np} );
            updates.Add(update);
        }

        VertexBlock block = new VertexBlock(new Random().Next(1, 100), new Random().Next(1, 100), hashes, updates);
        block.Sign(privateKey);

        VertexBlockMessage vbMsg = new(block);

        MemoryStream ms = new();

        // serialize with prefix
        Serializer.SerializeWithLengthPrefix<VertexBlockMessage>(ms, vbMsg, PrefixStyle.Base128, MessageTypeResolver.ResolveFieldNumber<VertexBlockMessage>());

        // deserialize
        ms.Position = 0;

        object deserialized;
        Assert.True(Serializer.NonGeneric.TryDeserializeWithLengthPrefix(ms, PrefixStyle.Base128, resolver, out deserialized));

        VertexBlock receivedBlock = ((VertexBlockMessage)deserialized).GetBlock();
        // check if fields are the same
        Assert.True(receivedBlock.Verify(publickey));

        Assert.True(block.digest.SequenceEqual(receivedBlock.digest));
        Assert.Equal(block.round, receivedBlock.round);
        Assert.Equal(block.source, receivedBlock.source);
        Assert.True(block.signature.SequenceEqual(receivedBlock.signature));
        for (int i = 0; i < block.updateMsgs.Count; i++)
        {
            Assert.True(block.updateMsgs[i].digest.SequenceEqual(receivedBlock.updateMsgs[i].digest));
        }
        for (int i = 0; i < block.prevCertificates.Count; i++)
        {
            Assert.True(block.prevCertificates[i].SequenceEqual(receivedBlock.prevCertificates[i]));
        }
    }

    [Fact]
    public void TestCertificateBlockMessage()
    {
        var blockhash = hashes[0];

        // create 10 private keys
        Dictionary<int, ECDsa> privateKeys = new();
        Dictionary<int, ECDsa> publicKeys = new();
        for (int i = 0; i < 10; i++)
        {
            privateKeys.Add(i, ECDsa.Create());
            ECDsa pk = ECDsa.Create();
            pk.ImportSubjectPublicKeyInfo(privateKeys[i].ExportSubjectPublicKeyInfo(), out _);
            publicKeys.Add(i, pk);
        }

        // create 10 signatures
        Dictionary<int, byte[]> signatures = new Dictionary<int, byte[]>();
        for (int i = 0; i < 10; i++)
        {
            signatures.Add(i, privateKeys[i].SignData(blockhash, HashAlgorithmName.SHA256));
        }

        Certificate block = new Certificate(new Random().Next(1, 100), new Random().Next(1, 100), blockhash, signatures);

        CertificateMessage cbMsg = new(block);

        MemoryStream ms = new();

        // serialize with prefix
        Serializer.SerializeWithLengthPrefix<CertificateMessage>(ms, cbMsg, PrefixStyle.Base128, MessageTypeResolver.ResolveFieldNumber<CertificateMessage>());

        // deserialize
        ms.Position = 0;

        object deserialized;
        Assert.True(Serializer.NonGeneric.TryDeserializeWithLengthPrefix(ms, PrefixStyle.Base128, resolver, out deserialized));

        Certificate receivedBlock = (Certificate)((CertificateMessage)deserialized).GetCertificate();
        // check if fields are the same

        Assert.Equal(block.round, receivedBlock.round);
        Assert.Equal(block.source, receivedBlock.source);
        Assert.True(block.blockHash.SequenceEqual(receivedBlock.blockHash));
    
        Assert.True(receivedBlock.CheckSignatures(publicKeys));

    }


    [Fact]
    public void TestSigMessage()
    {

        byte[] signature = privateKey.SignData(hashes[0], HashAlgorithmName.SHA256);

        SignatureMessage sigMsg = new(new Random().Next(1, 100), new Random().Next(1, 100), signature);

        MemoryStream ms = new();

        // serialize with prefix
        Serializer.SerializeWithLengthPrefix<SignatureMessage>(ms, sigMsg, PrefixStyle.Base128, MessageTypeResolver.ResolveFieldNumber<SignatureMessage>());

        // deserialize
        ms.Position = 0;

        object deserialized;
        Assert.True(Serializer.NonGeneric.TryDeserializeWithLengthPrefix(ms, PrefixStyle.Base128, resolver, out deserialized));

        SignatureMessage receivedSigMsg = (SignatureMessage)deserialized;
        // check if fields are the same
        Assert.Equal(sigMsg.round, receivedSigMsg.round);
        Assert.Equal(sigMsg.source, receivedSigMsg.source);
        Assert.True(sigMsg.signature.SequenceEqual(receivedSigMsg.signature));
        Assert.True(receivedSigMsg.Verify(hashes[0], publickey));
    }

    [Fact]
    public void TestMessagesOverTCP()
    {
        TcpListener server = new TcpListener(IPAddress.Parse("127.0.0.1"), 20000);

        List<UpdateMessage> updates = new List<UpdateMessage>();
        for (int i = 0; i < 10; i++)
        {
            UpdateMessage update = new UpdateMessage();
            update.digest = hashes[i];
            updates.Add(update);
        }

        VertexBlock block = new VertexBlock(new Random().Next(1, 100), new Random().Next(1, 100), hashes, updates);
        block.Sign(privateKey);

        bool flag = false;
        // create a listening loop in another thread using Task
        System.Threading.Tasks.Task.Run(() =>
        {
            server.Start();
            NetworkStream readStream = server.AcceptTcpClient().GetStream();

            // receive message
            object deserialized;
            while (Serializer.NonGeneric.TryDeserializeWithLengthPrefix(readStream, PrefixStyle.Base128, resolver, out deserialized))
            {
                VertexBlock receivedBlock = ((VertexBlockMessage)deserialized).GetBlock();
                // check if fields are the same

                Assert.True(receivedBlock.Verify(publickey));

                Assert.True(block.digest.SequenceEqual(receivedBlock.digest));
                Assert.Equal(block.round, receivedBlock.round);
                Assert.Equal(block.source, receivedBlock.source);
                Assert.True(block.signature.SequenceEqual(receivedBlock.signature));
                for (int i = 0; i < block.updateMsgs.Count; i++)
                {
                    Assert.True(block.updateMsgs[i].digest.SequenceEqual(receivedBlock.updateMsgs[i].digest));
                }
                for (int i = 0; i < block.prevCertificates.Count; i++)
                {
                    Assert.True(block.prevCertificates[i].SequenceEqual(receivedBlock.prevCertificates[i]));
                }

                flag = true;
                break;

            }
        });

        // wait 1 second
        Thread.Sleep(1000);
        TcpClient client = new TcpClient("127.0.0.1", 20000);
        NetworkStream writeStream = client.GetStream();

        VertexBlockMessage vbMsg = new(block);

        // serialize with prefix
        Serializer.SerializeWithLengthPrefix<VertexBlockMessage>(writeStream, vbMsg, PrefixStyle.Base128, MessageTypeResolver.ResolveFieldNumber<VertexBlockMessage>());


        while (!flag) { }

        client.Close();
        server.Stop();
    }



    public void Dispose()
    {

    }
}