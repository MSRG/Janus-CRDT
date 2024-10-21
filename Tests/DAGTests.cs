using Xunit;
using System;
using Microsoft.Extensions.Logging;
using BFTCRDT.DAG;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging.Debug;
using Xunit.Sdk;
using Planetarium.Cryptography.BLS12_381;

namespace Tests;

// adding this to prevent it from running parallel with the
// cluster tests
[Collection("CannotParallel")]
public class DAGTests : IDisposable
{
    List<DAG> dags;
    List<SimpleDAGMsgTestSender> senders;
    List<ILogger> loggers;
    int numberNodes = 4;

    public DAGTests()
    {
        this.dags = new();
        this.senders = new();
        this.loggers = new();
        
        DAGOptions conf = new(maxBatchSize: 1, blockInterval: 0);

        List<Replica> nodes = new();

        // generate a list of int from 0 to numberNodes with LinQ
        List<int> allNodeIDs = Enumerable.Range(0, numberNodes).ToList();

        foreach (int i in allNodeIDs)
        {
            nodes.Add(new Replica(i));
            senders.Add(new SimpleDAGMsgTestSender(allNodeIDs, nodes[i]));
            var lvl = LogLevel.None;
            if (i == 3)
            {
                lvl = LogLevel.None;
            }
            loggers.Add(LoggerFactory.Create(builder =>
            {
                LogLevel logLevel = lvl;
                builder.AddConsole().SetMinimumLevel(logLevel);
                builder.AddDebug().SetMinimumLevel(logLevel);

            }).CreateLogger("Node" + i));

        }

        for (int i = 0; i < numberNodes; i++)
        {
            dags.Add(new DAG(nodes[i], nodes, senders[i], loggers[i]));
            dags[i].options = conf;
            // using a fake init to initialize the DAG
            dags[i].ReceivedInit(new InitMessage(numberNodes - i % numberNodes - 1, null));

        }
    }

    [Fact]
    public void TestGenesis()
    {
        dags[0].Start();
        dags[1].Start();

        VertexBlockMessage msg = (VertexBlockMessage)senders[0].GetMessage(out _);
        VertexBlock genesis = msg.GetBlock();

        // check if the block created correctly
        // at round 0
        Assert.Equal("0", dags[0].getCurrRoundState()[0]);
        // digest should be the same
        Assert.Equal(System.Text.Encoding.Default.GetString(genesis.digest), dags[0].getCurrRoundState()[1]);
        // should received by self
        Assert.Equal("1", dags[0].getCurrRoundState()[3]);

        // check if 1 received it correctly
        dags[1].ReceivedBlock(genesis, false);
        // at round 0
        Assert.Equal("0", dags[1].getCurrRoundState()[0]);
        // should received
        Assert.Equal("2", dags[1].getCurrRoundState()[3]);

        // 1's last message should be a signature
        DAGMessage sigmsg = senders[1].GetMessage(out _, 1);
        Assert.IsType<SignatureMessage>(sigmsg);

        // 0 should accept the signature
        dags[0].ReceivedSignature((SignatureMessage)sigmsg);
        // should have two signatures (include self)
        Assert.Equal("2", dags[0].getCurrRoundState()[2]);
    }

    [Fact]
    public void TestGenerateCertificate()
    {
        dags[0].Start();
        dags[1].Start();
        dags[2].Start();

        VertexBlock genesis0 = ((VertexBlockMessage)senders[0].GetMessage(out _)).GetBlock(); ;

        dags[1].ReceivedBlock(genesis0, false);
        var sig1 = (SignatureMessage)senders[1].GetMessage(out _, 1);
        dags[2].ReceivedBlock(genesis0, false);
        var sig2 = (SignatureMessage)senders[2].GetMessage(out _, 1);


        dags[0].ReceivedSignature(sig1);
        dags[0].ReceivedSignature(sig2);

        // have 3 signatures, enough for 2f+1 to generate certificate
        DAGMessage certificate = senders[0].GetMessage(out _);
        Assert.IsType<CertificateMessage>(certificate);
        Certificate cert = ((CertificateMessage)certificate).GetCertificate();

        // check if the certificate is stored
        // check if the certificate is correct
        Assert.Equal(3, cert.receivedSignatures.Count);
        Assert.Equal(0, cert.source);
        Assert.Equal(0, cert.round);
        Assert.True(cert.blockHash.SequenceEqual(genesis0.digest));
        Assert.True(cert.receivedSignatures[1].SequenceEqual(sig1.signature));
        Assert.True(cert.receivedSignatures[2].SequenceEqual(sig2.signature));
    }

    [Fact]
    public void TestGoToRound1()
    {

        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
        }

        OneRound();

        Thread.Sleep(100);
        // everyone should be at round 1 
        foreach (var dag in dags)
        {
            dag.CheckCertificates();
            VerifyAtRound(1);
        }

    }

    [Fact]
    public void TestFirstConsensus()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            Consensus cons = new(dags[i], loggers[i]);
            dags[i].consensus = cons;

            dags[i].Start();
        }

        // first 3 rounds
        for (int r = 0; r < 2; r++)
        {
            OneRound();
        }

        Assert.Equal("2", dags[0].getCurrRoundState()[0]);

        // check if all have the same block
        var orderedBlocksOfNode0 = dags[0].consensus.orderedBlocks;
        foreach (var dag in dags)
        {
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;
                Assert.Equal(orderedBlocksOfNode0[0].blockHash, orderedBlocksOfNodei[0].blockHash, new BFTCRDT.ByteArrayComparer());
            }
        }
    }


    [Fact]
    public void TestFewConsensus()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            Consensus cons = new(dags[i], loggers[i]);
            dags[i].consensus = cons;
            dags[i].options = new DAGOptions(maxBatchSize: 10, blockInterval: 100);
            dags[i].Start();
        }

        // first 4 waves
        for (int r = 0; r < 8; r++)
        {
            OneRound();
        }
        
        Thread.Sleep(100);
        Assert.Equal("8", dags[0].getCurrRoundState()[0]);

        // check if all have the same block
        var orderedBlocksOfNode0 = dags[0].consensus.orderedBlocks;

        // should have 25 blocks committed at round 8, leader at round 6, 6 * 4 + 1 = 25
        Assert.Equal(25, orderedBlocksOfNode0.Count);

        foreach (var dag in dags)
        {
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;
                Assert.Equal(orderedBlocksOfNode0[0].blockHash, orderedBlocksOfNodei[0].blockHash, new BFTCRDT.ByteArrayComparer());
            }
        }
    }

    [Fact]
    public void TestKeepGoingTo100()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            Consensus cons = new(dags[i]);
            dags[i].consensus = cons;

            dags[i].Start();
        }

        for (int r = 0; r < 100; r++)
        {
            OneRound();
        }

        // check if all at around 100
        foreach (var dag in dags)
        {
            Assert.Equal("100", dag.getCurrRoundState()[0]);
        }




    }
    
    [Fact]
    public void TestLargeConsensus()
    {
        TestKeepGoingTo100();
        var orderedBlocksOfNode0 = dags[0].consensus.orderedBlocks;


        foreach (var dag in dags)
        {
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;
                for (int i = 0; i < orderedBlocksOfNodei.Count; i++)
                {
                    Assert.Equal(orderedBlocksOfNode0[i].blockHash, orderedBlocksOfNodei[i].blockHash, new BFTCRDT.ByteArrayComparer());
                }
            }
        }
    }

    [Fact]
    public void TestOnlyReceived2Certs()
    {
        List<VertexBlockMessage> blockmsgs = new();

        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
            var msg = (VertexBlockMessage)senders[i].GetMessage(out _);
            blockmsgs.Add(msg);
        }

        for (int i = 0; i < numberNodes; i++)
        {
            Assert.Equal("0", dags[i].getCurrRoundState()[0]);
        }

        // send each other messages
        for (int i = 0; i < numberNodes; i++)
        {
            // send other nodes genesis block
            for (int j = 0; j < numberNodes; j++)
            {
                if (i != j)
                {
                    dags[j].ReceivedBlock(blockmsgs[i].GetBlock(), false);
                }
            }
        }

        // gather return signatures
        List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
        for (int i = 0; i < numberNodes; i++)
        {
            sigmsgs.Add(new());

            while (senders[i].msgQueue.Count > 0)
            {
                var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                sigmsgs[i].Add((sigmsg, receivers[0]));
            }
        }

        // send back signatures
        for (int i = 0; i < numberNodes; i++)
        {
            // each replica should contain 3 signatures to send
            Assert.Equal(3, sigmsgs[i].Count);

            foreach (var sigmsg in sigmsgs[i])
            {
                dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
            }
        }

        for (int i = 0; i < numberNodes; i++)
        {
            // only send 1 certificates
            var certMsg = (CertificateMessage)senders[i].GetMessage(out _);

            int j = i + 1 < numberNodes ? i + 1 : 0;
            dags[j].ReceivedCertificate(certMsg.GetCertificate());
        }

        for (int i = 0; i < numberNodes; i++)
        {
            Assert.Equal("0", dags[i].getCurrRoundState()[0]);
            Assert.Null(senders[i].GetMessage(out _));
        }


    }

    [Fact]
    public void TestReceivedCertsWithoutPervs()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
        }

        OneRound();

        // at round 1
        SendBlocks();
        SendSignatures();

        
        // everyone should be at round 1 
        foreach (var dag in dags)
        {
            dag.CheckCertificates();
            Assert.Equal("1", dag.getCurrRoundState()[0]);
        }

        // gather certificates 
        List<Certificate> certs = new();
        for (int i = 0; i < numberNodes; i++)
        {
            var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
            certs.Add(certmsg.GetCertificate());
        }

        List<Certificate> temp = new();
        for (int i = 0; i < numberNodes; i++)
        {
            for (int j = 0; j < numberNodes; j++)
            {
                // let 0 misses 1's certificate for round 1
                if (j == 0 && i == 1)
                {
                    temp.Add(certs[i]);
                    continue;
                }

                if (i != j)
                {
                    dags[j].ReceivedCertificate(certs[i]);
                }
            }
        }

        // everyone should be at round 2
        foreach (var dag in dags)
        {
            dag.CheckCertificates();
            Assert.Equal("2", dag.getCurrRoundState()[0]);
        }

        // another round
        SendBlocks();
        SendSignatures();

        // gather certificates 
        certs = new();
        for (int i = 0; i < numberNodes; i++)
        {
            var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
            certs.Add(certmsg.GetCertificate());
        }

        for (int i = 0; i < numberNodes; i++)
        {
            for (int j = 0; j < numberNodes; j++)
            {
                if (i != j)
                {
                    dags[j].ReceivedCertificate(certs[i]);
                }
            }
        }

        // O should not go to round 3 because all certificates has 1 from round 1, so those
        // certificates are not accepted yet
        // but everyone should be at round 2
        Assert.Equal("2", dags[0].getCurrRoundState()[0]);

        for (int i = 1; i < numberNodes; i++)
        {
            Assert.Equal("3", dags[i].getCurrRoundState()[0]);
        }
              

        // have 0 recieves the certificate from 1 again
        foreach (var item in temp)
        {
            dags[0].ReceivedCertificate(item);
        }

        Thread.Sleep(100);
        // 0 should be at round 3
        Assert.Equal("3", dags[0].getCurrRoundState()[0]);


    }

    [Fact]
    public void TestReceivedCertOfFutureRound()
    {

        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
        }

        OneRound();
        OneRound();

        // every one should at round 2 now
        for (int i = 0; i < numberNodes; i++)
        {
            Assert.Equal("2", dags[i].getCurrRoundState()[0]);
        }

        SendBlocks();
        SendSignatures();

        // gather certificates 
        List<Certificate> certs = new();
        List<Certificate> temp = new();
        for (int i = 0; i < numberNodes; i++)
        {
            var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
            certs.Add(certmsg.GetCertificate());
        }

        for (int i = 0; i < numberNodes; i++)
        {
            for (int j = 0; j < numberNodes; j++)
            {   
                if (j == 0 && i != 0)
                {
                    temp.Add(certs[i]);
                }

                if (i != j && j != 0)
                {
                    dags[j].ReceivedCertificate(certs[i]);
                }
            }
        }

        // O should not go to round 3 because all certificates has 1 from round 1, so those
        // certificates are not accepted yet
        // but everyone should be at round 3
        Assert.Equal("2", dags[0].getCurrRoundState()[0]);

        for (int i = 1; i < numberNodes; i++)
        {
            Assert.Equal("3", dags[i].getCurrRoundState()[0]);
        }

        // others sending each other blocks
        for (int i = 1; i < numberNodes; i++)
        {
            var blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
            Assert.NotNull(blockmsg);

            for (int j = 0; j < numberNodes; j++)
            {
                if (i != j)
                {
                    dags[j].ReceivedBlock(blockmsg.GetBlock(), false);
                }
            }
        }

        // others sending each other sigs
        // gather return signatures
        List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new()
        {
            new()
        };
        for (int i = 1; i < numberNodes; i++)
        {
            sigmsgs.Add(new());

            while (senders[i].msgQueue.Count > 0)
            {
                var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                sigmsgs[i].Add((sigmsg, receivers[0]));
            }
        }

        // send back signatures
        for (int i = 1; i < numberNodes; i++)
        {
            // each replica should contain 3 signatures to send
            Assert.Equal(2, sigmsgs[i].Count);

            foreach (var sigmsg in sigmsgs[i])
            {
                dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
            }
        }

        // gather certs for round 3 - node 0 is ignored
        certs.Clear();
        certs.Add(null); // padding 0
        for (int i = 1; i < numberNodes; i++)
        {
            var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
            certs.Add(certmsg.GetCertificate());
        }

        // broacast certificates
        for (int i = 1; i < numberNodes; i++)
        {
            for (int j = 0; j < numberNodes; j++)
            {
                if (i != j)
                {
                    dags[j].ReceivedCertificate(certs[i]);
                }
            }
        }

        Thread.Sleep(100);
        // everyone else at round 4
        for (int i = 1; i < numberNodes; i++)
        {
            Assert.Equal("4", dags[i].getCurrRoundState()[0]);
        }

        // node 0 should not advance based on received round 3 certs
        Assert.Equal("2", dags[0].getCurrRoundState()[0]);
        
        // have 0 receives the certificate from 1 again
        foreach (var item in temp)
        {
            dags[0].ReceivedCertificate(item);
        }

        Thread.Sleep(100);
        // node 0 should advance to round 4
        Assert.Equal("4", dags[0].getCurrRoundState()[0]);


        

    }
    
    [Fact]
    public void TestIncorrectACK()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
        }

        // one round
        OneRound();
        VerifyAtRound(1);

        // second round
        SendBlocks();

        // sig from 1, set it to incorrect round
        var sigmsg = (SignatureMessage)senders[1].GetMessage(out List<int> receivers);
        SignatureMessage newmsg = new(sigmsg.source, 0, sigmsg.signature);
        dags[0].ReceivedSignature(newmsg);
        Assert.Equal("1", dags[0].getCurrRoundState()[2]);
    }

    [Fact]
    public void TestJumpingRounds()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Start();
        }


        Queue<Certificate> missingCertsFor3 = new();

        // one round
        OneRound();
        VerifyAtRound(1);

        // go to round 2 with only 0,1,2
        {
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
                Assert.NotNull(blockmsg);

                for (int j = 0; j < numberNodes; j++)
                {
                    if (i != j)
                    {
                        dags[j].ReceivedBlock(blockmsg.GetBlock(), false);
                    }
                }
            }

            // gather return signatures
            List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                sigmsgs.Add(new());

                while (senders[i].msgQueue.Count > 0)
                {
                    var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                    sigmsgs[i].Add((sigmsg, receivers[0]));
                }
            }

            // send back signatures
            for (int i = 0; i < numberNodes - 1; i++)
            {
                // each replica should contain 3 signatures to send
                Assert.Equal(2, sigmsgs[i].Count);

                foreach (var sigmsg in sigmsgs[i])
                {
                    dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
                }
            }

            // gather certificates 
            List<Certificate> certs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
                certs.Add(certmsg.GetCertificate());
            }

            // broacast certificates
            for (int i = 0; i < numberNodes - 1; i++)
            {
                for (int j = 0; j < numberNodes - 1; j++)
                {
                    if (i != j)
                    {
                        Certificate copy = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                        dags[j].ReceivedCertificate(copy);
                    }
                }
                Certificate copy1 = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                missingCertsFor3.Enqueue(copy1);
            }

            // 0, 1, 2 should be at round 2
            VerifyAtRound(2, new int[] { 3 });

        }

        // go to round 3 with only 0,1,2
        {
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
                Assert.NotNull(blockmsg);

                for (int j = 0; j < numberNodes; j++)
                {
                    if (i != j)
                    {
                        dags[j].ReceivedBlock(blockmsg.GetBlock(), false);
                    }
                }
            }

            // gather return signatures
            List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                sigmsgs.Add(new());

                while (senders[i].msgQueue.Count > 0)
                {
                    var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                    sigmsgs[i].Add((sigmsg, receivers[0]));
                }
            }

            // send back signatures
            for (int i = 0; i < numberNodes - 1; i++)
            {
                // each replica should contain 3 signatures to send
                Assert.Equal(2, sigmsgs[i].Count);

                foreach (var sigmsg in sigmsgs[i])
                {
                    dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
                }
            }

            // gather certificates 
             List<Certificate> certs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
                certs.Add(certmsg.GetCertificate());
            }

            // broacast certificates
            for (int i = 0; i < numberNodes - 1; i++)
            {
                for (int j = 0; j < numberNodes - 1; j++)
                {
                    if (i != j)
                    {
                        Certificate copy = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                        dags[j].ReceivedCertificate(copy);
                    }
                }
                Certificate copy1 = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                missingCertsFor3.Enqueue(copy1);
            }

            // 0, 1, 2 should be at round 2
            VerifyAtRound(3, new int[] { 3 });
        }

        // all exchange round 3 blocks
        {
            SendBlocks();
            

            List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
            for (int i = 0; i < numberNodes; i++)
            {
                sigmsgs.Add(new());

                while (senders[i].msgQueue.Count > 0)
                {
                    var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                    sigmsgs[i].Add((sigmsg, receivers[0]));
                }
            }

            // send back signatures
            for (int i = 0; i < numberNodes; i++)
            {
                foreach (var sigmsg in sigmsgs[i])
                {
                    dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
                }
            }   


            // gather certificates 
            List<Certificate> certs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
                certs.Add(certmsg.GetCertificate());
            }

            // broacast certificates
            for (int i = 0; i < numberNodes - 1; i++)
            {
                for (int j = 0; j < numberNodes; j++)
                {
                    if (i != j)
                    {
                        Certificate copy = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                        dags[j].ReceivedCertificate(copy);
                    }
                }
            }

            while (missingCertsFor3.Count > 0)
            {
                var cert = missingCertsFor3.Dequeue();
                dags[3].ReceivedCertificate(cert);
            }

            VerifyAtRound(4);
        }

        // OneRound();
        // VerifyAtRound(5);
    }

    [Fact]
    public void TestPruningInvalidBlocks()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            Consensus cons = new(dags[i], loggers[i]);
            dags[i].consensus = cons;
            dags[i].Start();
        }


        Queue<Certificate> missingCertsFor3 = new();

        // one round
        OneRound();
        VerifyAtRound(1);

        // one round without node 3's cert
        var OneRoundWithout3 = () => {
            for (int i = 0; i < numberNodes; i++)
            {
                var blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
                Assert.NotNull(blockmsg);

                for (int j = 0; j < numberNodes; j++)
                {
                    if (i != j)
                    {
                        dags[j].ReceivedBlock(blockmsg.GetBlock(), false);
                    }
                }
            }

            // gather return signatures
            List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
            for (int i = 0; i < numberNodes; i++)
            {
                sigmsgs.Add(new());

                while (senders[i].msgQueue.Count > 0)
                {
                    var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                    sigmsgs[i].Add((sigmsg, receivers[0]));
                }
            }

            // send back signatures
            for (int i = 0; i < numberNodes; i++)
            {
                Assert.Equal(3, sigmsgs[i].Count);

                foreach (var sigmsg in sigmsgs[i])
                {
                    dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
                }
            }

            // gather certificates, node 3 does not send its certificate
            List<Certificate> certs = new();
            for (int i = 0; i < numberNodes - 1; i++)
            {
                var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
                certs.Add(certmsg.GetCertificate());
            }

            // clear the certificate for node 3
            senders[3].GetMessage(out _);

            // broacast certificates
            for (int i = 0; i < numberNodes - 1; i++)
            {
                for (int j = 0; j < numberNodes; j++)
                {
                    if (i != j)
                    {
                        Certificate copy = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                        dags[j].ReceivedCertificate(copy);
                    }
                }
                Certificate copy1 = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                missingCertsFor3.Enqueue(copy1);
            }
        };
        
        // r2
        OneRoundWithout3();
        // r3
        OneRoundWithout3();
        // r4
        OneRoundWithout3();
        VerifyAtRound(4);   

        // one round with node 3's cert
        OneRound();
        VerifyAtRound(5);

        // round 1-3 are invalid
        var invalidblocks = dags[0].PruneInvalidBlocks();
        Assert.Equal(3, invalidblocks.Count); 
        Assert.Equal(3, invalidblocks[0].round);
        Assert.Equal(2, invalidblocks[1].round);
        Assert.Equal(1, invalidblocks[2].round);
        // all blocks should be from node 3
        invalidblocks.All(b => b.source == 3);

        // blocks should be same for other nodes
        for (int i = 1; i < numberNodes - 1; i++)
        {
            var invalidblocks2 = dags[i].PruneInvalidBlocks();
            Assert.Equal(invalidblocks.Count, invalidblocks2.Count);
            for (int j = 0; j < invalidblocks.Count; j++)
            {
                Assert.True(invalidblocks[i].digest.SequenceEqual(invalidblocks2[i].digest));
            }
        }
    }

               

    private void SendBlocks()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            Stopwatch sw = new();
            sw.Start();
            var blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
            while (blockmsg == null)
            {
                blockmsg = (VertexBlockMessage)senders[i].GetMessage(out _);
                if (sw.ElapsedMilliseconds > 10000)
                {
                    throw new Exception("Timeout");
                }
            }
            Assert.NotNull(blockmsg);

            for (int j = 0; j < numberNodes; j++)
            {
                if (i != j)
                {
                    VertexBlock b = blockmsg.GetBlock();
                    VertexBlock copy = new(b.round, b.source, b.prevCertificates, b.updateMsgs, b.digest)
                    {
                        signature = b.signature
                    };
                    dags[j].ReceivedBlock(copy, false);
                }
            }
        }
    }

    private void SendSignatures()
    {
        // gather return signatures
        List<List<(SignatureMessage msg, int receiver)>> sigmsgs = new();
        for (int i = 0; i < numberNodes; i++)
        {
            sigmsgs.Add(new());

            while (senders[i].msgQueue.Count > 0)
            {
                var sigmsg = (SignatureMessage)senders[i].GetMessage(out List<int> receivers);
                sigmsgs[i].Add((sigmsg, receivers[0]));
            }
        }

        // send back signatures
        for (int i = 0; i < numberNodes; i++)
        {
            // each replica should contain 3 signatures to send
            Assert.Equal(3, sigmsgs[i].Count);

            foreach (var sigmsg in sigmsgs[i])
            {
                dags[sigmsg.receiver].ReceivedSignature(sigmsg.msg);
            }
        }
    }

    private void SendCertificates()
    {
        // gather certificates 
        List<Certificate> certs = new();
        for (int i = 0; i < numberNodes; i++)
        {
            var certmsg = (CertificateMessage)senders[i].GetMessage(out _);
            certs.Add(certmsg.GetCertificate());
        }

        // broacast certificates
        for (int i = 0; i < numberNodes; i++)
        {
            int k = 0;
            for (int j = i; k < numberNodes; j++)
            {
                if (j == numberNodes)
                    j = 0;
            
                if (i != j)
                {
                    Certificate copy = new(certs[i].round, certs[i].source, certs[i].blockHash, certs[i].receivedSignatures);
                    dags[j].ReceivedCertificate(copy);
                }
                k++;
            }
        }
    }

    private void OneRound()
    {
        SendBlocks();
        SendSignatures();
        SendCertificates();
        Console.WriteLine("One round done");
    }   

    /// <summary>
    /// Verify if dags are at a given round. notCheck is a list of node that should not be checked.
    /// </summary>
    /// <param name="round">round number</param>
    /// <param name="NotCheck">list of nodes ids that are not check</param>
    private void VerifyAtRound(int round, int[] notCheck = null)
    {
        Thread.Sleep(100);

        if (notCheck == null)
        {
            notCheck = new int[] { };
        }

        // check at round round
        foreach (var dag in dags)
        {   
            if (!notCheck.Contains(dag.self.nodeid))
            {
                Assert.True(round.ToString().Equals(dag.getCurrRoundState()[0]), $"Expected round {round}, got {dag.getCurrRoundState()[0]} at {dag.self.nodeid}");
            }
        }
    }

    public void Dispose()
    {
        for (int i = 0; i < numberNodes; i++)
        {
            dags[i].Stop();
        }
    }
}

// Manual control of the messages
public class SimpleDAGMsgTestSender : IDAGMsgSender
{
    public Replica selfNode { get; set; }
    public List<int> allNodeIDs { get; set; }
    public Queue<(DAGMessage msg, List<int> receivers)> msgQueue { get; set; }


    public SimpleDAGMsgTestSender(List<int> allNodeIDs, Replica selfNode)
    {
        this.selfNode = selfNode;
        this.msgQueue = new();

        this.allNodeIDs = allNodeIDs;
    }

    public void BroadcastBlock(VertexBlock block)
    {
        VertexBlockMessage msg = new(block);
        msgQueue.Enqueue((msg, allNodeIDs));
    }

    public void BroadcastCertificate(Certificate certificate)
    {
        CertificateMessage msg = new(certificate);
        msgQueue.Enqueue((msg, allNodeIDs));
    }

    public void QueryForBlock(byte[] blockhash, int[] receivers)
    {
        BlockQueryMessage msgToSend = new(blockhash, selfNode.nodeid);
        msgQueue.Enqueue((msgToSend, receivers.ToList()));
    }

    public void SendBlock(VertexBlock block, int receiver, bool IsQueryReply = false)
    {
        VertexBlockMessage msg = new(block, IsQueryReply);
        msgQueue.Enqueue((msg, new List<int>() { receiver }));

    }

    public void SendSignature(int round, byte[] signature, int receiver)
    {
        SignatureMessage msg = new(selfNode.nodeid, round, signature);
        msgQueue.Enqueue((msg, new List<int>() { receiver }));
    }

    /// <summary>
    /// Get the message from the queue where the dag pushes to send
    /// </summary>
    /// <param name="receivers">receiving node id</param>
    /// <param name="i"># of messages to get</param>
    /// <returns></returns>
    public DAGMessage GetMessage(out List<int> receivers, int i = 0)
    {
        receivers = null;

        for (int j = 0; j < i; j++)
        {
            msgQueue.Dequeue();
        }
        if (msgQueue.Count == 0)
            return null;

        var msg = msgQueue.Dequeue();
        receivers = msg.receivers;
        return msg.msg;
    }

    public void ClearQueue()
    {
        msgQueue.Clear();
    }

    public void BroadCastInit(int source, byte[] publicKey)
    {
        throw new NotImplementedException();
    }
}





// adding this to prevent it from running parallel with the
// cluster tests
[Collection("CannotParallel")]
public class DAGTestNoManualMessaging : IDisposable
{
    Dictionary<int, DAG> cluster;
    Dictionary<int, FullDAGMsgTestSender> allSenders;
    Dictionary<int, ConcurrentQueue<DAGMessage>> msgQueue;
    List<ILogger> loggers;
    int numberNodes = 4;


    public DAGTestNoManualMessaging()
    {
        this.cluster = new();
        this.allSenders = new();
        this.msgQueue = new();
        this.loggers = new();

        DAGOptions option = new(maxBatchSize: 1, blockInterval: 0);

        List<Replica> nodes = new();
        for (int i = 0; i < numberNodes; i++)
        {
            nodes.Add(new Replica(i));
            allSenders[i] = new FullDAGMsgTestSender(nodes[i]);
            msgQueue[i] = new ConcurrentQueue<DAGMessage>();
            var lvl = LogLevel.None;
            if (i == 1)
            {
                lvl = LogLevel.None;
            }
            loggers.Add(LoggerFactory.Create(builder =>
            {
                LogLevel logLevel = lvl;
                builder.AddConsole().SetMinimumLevel(logLevel);
                builder.AddDebug().SetMinimumLevel(logLevel);
            }).CreateLogger("Node" + i));

        }

        for (int i = 0; i < numberNodes; i++)
        {
            cluster[i] = new DAG(nodes[i], nodes, allSenders[i], loggers[i]);
            cluster[i].options = option;
            Consensus c = new(cluster[i]);
            cluster[i].consensus = c;
            cluster[i].ReceivedInit(new InitMessage(numberNodes - i % numberNodes - 1, null));
        }

        foreach (var k in allSenders.Values)
        {
            k.allDAGs = cluster;
            k.msgQueue = msgQueue;
        }

    }



    volatile bool running = true;
    [Fact]
    public async void TestRun()
    {
        List<Thread> threads = new();

        foreach (var dag in cluster.Values)
        {
            threads.Add(new Thread(() =>
            {
                dag.Start();
                Thread.Sleep(1000);
                while (running)
                {
                    if (msgQueue[dag.self.nodeid].TryDequeue(out var msg))
                    {
                        dag.HandleMessage(msg);
                    }
                }
            }));
        }

        loggers[0].LogDebug("Starting threads");
        // starting threads
        running = true;
        foreach (var t in threads)
        {
            t.Start();
        }

        Thread.Sleep(10000);

        // Stop all tasks

        running = false;
        loggers[0].LogDebug("Stopping tasks");

        foreach (var t in threads)
        {
            t.Join();
        }

        List<int> rounds = new();
        foreach (var dag in cluster.Values)
        {
            rounds.Add(int.Parse(dag.getCurrRoundState()[0]));
        }

        string s = string.Join(",", rounds);

        // check if all are over 50 rounds
        Assert.True(rounds.All(x => x >= 50), $"Not all nodes are over 50 rounds, rounds are: {s}");
    }

    [Fact]
    public void TestConsensus()
    {
        TestRun();
        var orderedBlocksOfNode0 = cluster[0].consensus.orderedBlocks;

        foreach (var dag in cluster.Values)
        {
            if (dag.self.nodeid != 0)
            {
                var orderedBlocksOfNodei = dag.consensus.orderedBlocks;

                for (int i = 0; i < 100; i++)
                {
                    Assert.Equal(orderedBlocksOfNode0[i].blockHash, orderedBlocksOfNodei[i].blockHash, new BFTCRDT.ByteArrayComparer());
                }
            }
        }
    }


    public void Dispose()
    {
        foreach (var dag in cluster.Values)
        {
            dag.Stop();
        }
    }
}


public class FaultyDAGTests : IDisposable
{
    Dictionary<int, DAG> dags;
    Dictionary<int, FullDAGMsgTestSender> allSenders;
    Dictionary<int, ConcurrentQueue<DAGMessage>> msgQueue;
    List<ILogger> loggers;
    int numberNodes = 4;


    public FaultyDAGTests()
    {
        this.dags = new();
        this.allSenders = new();
        this.msgQueue = new();
        this.loggers = new();

        DAGOptions option = new(maxBatchSize: 1, blockInterval: 50);

        List<Replica> nodes = new();
        for (int i = 0; i < numberNodes; i++)
        {
            nodes.Add(new Replica(i));
            allSenders[i] = new FullDAGMsgTestSender(nodes[i]);
            msgQueue[i] = new ConcurrentQueue<DAGMessage>();
            var lvl = LogLevel.None;
            if (i == 3)
            {
                lvl = LogLevel.None;
            }
            loggers.Add(LoggerFactory.Create(builder =>
            {
                LogLevel logLevel = lvl;
                builder.AddConsole().SetMinimumLevel(logLevel);
                builder.AddDebug().SetMinimumLevel(logLevel);
            }).CreateLogger("Node" + i));

        }

        for (int i = 0; i < numberNodes; i++)
        {
            dags[i] = new DAG(nodes[i], nodes, allSenders[i], loggers[i])
            {
                options = option
            };
            Consensus c = new(dags[i]);
            dags[i].consensus = c;
            dags[i].ReceivedInit(new InitMessage(numberNodes - i % numberNodes - 1, null));
        }

        dags[numberNodes - 1].options = new(1, 50, 50);

        foreach (var k in allSenders.Values)
        {
            k.allDAGs = dags;
            k.msgQueue = msgQueue;
        }

    }

    volatile bool running = true;
    [Fact]
    public async void TestRun()
    {
        List<Thread> threads = new();

        foreach (var dag in dags.Values)
        {
            threads.Add(new Thread(() =>
            {
                dag.Start();
                Thread.Sleep(1000);
                while (running)
                {
                    if (msgQueue[dag.self.nodeid].TryDequeue(out var msg))
                    {
                        dag.HandleMessage(msg);
                    }
                }
            }));
        }

        loggers[0].LogDebug("Starting threads");
        // starting threads
        running = true;
        foreach (var t in threads)
        {
            t.Start();
        }

        Thread.Sleep(5000);

        // Stop all tasks

        running = false;
        loggers[0].LogDebug("Stopping tasks");

        foreach (var t in threads)
        {
            t.Join();
        }

        List<int> rounds = new();
        foreach (var dag in dags.Values)
        {
            rounds.Add(int.Parse(dag.getCurrRoundState()[0]));
        }

        string s = string.Join(",", rounds);

        // check if all are over 50 rounds
        Assert.True(rounds.All(x => x >= 50), $"Not all nodes are over 50 rounds, rounds are: {s}");
    }

    [Fact]
    public void TestPruneInvalidBlocks()
    {
        TestRun();
        List<List<VertexBlock>> invalidblocks = new();

        for (int i = 0; i < numberNodes - 1; i++)
        {
            invalidblocks.Add(dags[i].PruneInvalidBlocks());
        }

        // We should have **roughly** have half of the blocks invalid
        Assert.True(int.Parse(dags[0].getCurrRoundState()[0]) / 2 - invalidblocks[0].Count < 10);
        Assert.True(invalidblocks.All(b => b.Count == invalidblocks[0].Count));

        for (int i = 0; i < numberNodes - 1; i++)
        {
            for (int j = 0; j < invalidblocks[0].Count; j++)
            {
                var b = invalidblocks[i][j];
                Assert.True(b.source == numberNodes - 1); // all must from the last node
            }
        }
    }


    public void Dispose()
    {
        foreach (var dag in dags.Values)
        {
            dag.Stop();
        }
    }
}

public class FullDAGMsgTestSender : IDAGMsgSender
{
    public Replica selfNode { get; set; }

    public Dictionary<int, ConcurrentQueue<DAGMessage>> msgQueue { get; set; }
    // <node id, DAG>
    public Dictionary<int, DAG> allDAGs { get; set; }

    public FullDAGMsgTestSender(Replica selfNode)
    {
        this.selfNode = selfNode;
    }

    public void BroadcastBlock(VertexBlock block)
    {
       
        Parallel.ForEach(msgQueue, queue =>
        {
            if (queue.Key != selfNode.nodeid)
            {
                VertexBlock copy = new(block.round, block.source, block.prevCertificates, block.updateMsgs, block.digest)
                {
                    signature = block.signature
                };
                VertexBlockMessage msg = new(block);
                queue.Value.Enqueue(msg);
            }
        });
    }

    public void BroadcastCertificate(Certificate certificate)
    {
        
        Parallel.ForEach(msgQueue, queue =>
        {
            if (queue.Key != selfNode.nodeid)
            {
                Certificate copy = new(certificate.round, certificate.source, certificate.blockHash, certificate.receivedSignatures);
                CertificateMessage msg = new(certificate);
                queue.Value.Enqueue(msg);
            }
        });
    }

    public void QueryForBlock(byte[] blockhash, int[] receivers)
    {
        Parallel.ForEach(msgQueue, queue =>
        {
            if (queue.Key != selfNode.nodeid && receivers.Contains(queue.Key))
                queue.Value.Enqueue(new BlockQueryMessage(blockhash, selfNode.nodeid));
        });

    }

    public void SendBlock(VertexBlock block, int receiver, bool IsQueryReply = false)
    {
        VertexBlock copy = new(block.round, block.source, block.prevCertificates, block.updateMsgs, block.digest)
        {
            signature = block.signature
        };

        msgQueue[receiver].Enqueue(new VertexBlockMessage(block, IsQueryReply));
    }

    public void SendSignature(int round, byte[] signature, int receiver)
    {
        SignatureMessage msg = new(selfNode.nodeid, round, signature);

        msgQueue[receiver].Enqueue(msg);
    }

    public void BroadCastInit(int source, byte[] publicKey)
    {
        throw new NotImplementedException();
    }
}