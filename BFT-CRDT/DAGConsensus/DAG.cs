using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Diagnostics;
using System.Threading.Tasks;

namespace BFTCRDT.DAG;

public class DAGOptions
{
    // number of updates in a block 
    public readonly int maxBatchSize;
    // the interval to create a new block in ms
    public readonly int blockInterval;
    // the percentage of certificated that are not sent
    // to simulate a faulty node range from 0 to 100
    // because a faulty certificate will never be accepted
    public readonly int faultyRate;

    public DAGOptions(int maxBatchSize = 100,
                        int blockInterval = 50,
                        int faultyRate = 0)
    {
        this.maxBatchSize = maxBatchSize;
        this.blockInterval = blockInterval;
        this.faultyRate = faultyRate;
    }
}

internal class CurrentRoundState
{
    public int round { get; set; } = 0;
    public VertexBlock currentRoundBlock { get; set; } = null;
    // received acks for the current round block
    public ConcurrentDictionary<int, byte[]> receivedAcks { get; set; }
    public bool didConsensus { get; set; } = false;

    public AutoResetEvent canAdvanceRoundNotifier = new(false);
    public int enoughCertForRoundi = -1;

    public CurrentRoundState()
    {
        receivedAcks = new ConcurrentDictionary<int, byte[]>();
    }

    public void ToRound(int round)
    {
        this.round = round;
        receivedAcks.Clear();
        currentRoundBlock = null;
        didConsensus = false;
    }
}

public delegate void ReceivedBlockCallback(VertexBlock block);

/// <summary>
/// A DAG construct
/// </summary>
public class DAG
{
    // cluster infos
    public Replica self { get; }
    // <nodeid, DAGNode>
    public Committee committee { get; }
    public Consensus consensus { get; set; } = null;
    // responsible for sending messages
    private readonly IDAGMsgSender msgSender;

    public int f { get; }
    public bool isInit { get; private set; } = false;
    private CancellationTokenSource active = new();

    public DAGOptions options = new();
    private readonly Stopwatch blockCreationTimer = new();

    private int lastCollectedRound = 0;
    private int lastPrunedRound = 0;

    private CurrentRoundState currentRoundState;

    // if I received a certificated, but not its predecessors, use a priority queue 
    // so that certificates from earlier rounds are processed first
    private PriorityQueue<Certificate, int> certificatesBuffer;

    private LinkedList<UpdateMessage> updatesBuffer;

    // blocks index in the form of <round, <source, block>>
    private readonly List<ConcurrentDictionary<int, VertexBlock>> receivedBlocks;
    private readonly List<ConcurrentDictionary<int, Certificate>> receivedCertificates;

    // blocks index in the form of <hash, Block>, only a block with a certificate can be indexed
    public ConcurrentDictionary<byte[], VertexBlock> blocksIndexedByHash { get; private set; }
    public ConcurrentDictionary<byte[], Certificate> certificatesIndexedByHash;

    public ReceivedBlockCallback receivedBlockEvent { set; get; }

    private readonly ILogger logger;
    private Stats stats;

    private SpinLock spinLock0 = new();
    private ReaderWriterLockSlim advandRoundLock = new ReaderWriterLockSlim();

    private readonly Random random = new();

    public DAG(Replica selfNode, List<Replica> committee, IDAGMsgSender msgSender, ILogger logger = null)
    {
        this.self = selfNode;
        this.committee = new Committee(committee);

        this.currentRoundState = new CurrentRoundState();
        f = (this.committee.numNodes - 1) / 3;

        this.updatesBuffer = new LinkedList<UpdateMessage>();
        this.certificatesBuffer = new PriorityQueue<Certificate, int>();

        this.receivedBlocks = new List<ConcurrentDictionary<int, VertexBlock>>
        {
            new(this.committee.numNodes, this.committee.numNodes) // round 0
        };
        this.receivedCertificates = new List<ConcurrentDictionary<int, Certificate>>
        {
            new(this.committee.numNodes, this.committee.numNodes)
        };

        this.blocksIndexedByHash = new ConcurrentDictionary<byte[], VertexBlock>(new ByteArrayComparer());
        this.certificatesIndexedByHash = new ConcurrentDictionary<byte[], Certificate>(new ByteArrayComparer());

        blockCreationTimer.Start();

        this.msgSender = msgSender;
        this.stats = new Stats();
        this.logger = logger ?? new NullLogger<DAG>();
    }


    public void DAGClusterInit()
    {
        msgSender.BroadCastInit(self.nodeid, self.publicKey);
    }

    public void Start()
    {
        //consensus.stats = this.stats;


        logger.LogInformation("Replica {nodeid} started constructing DAG", self.nodeid);
        var genesisBlock = CreateBlock(new List<byte[]>(), new List<UpdateMessage>());
        msgSender.BroadcastBlock(genesisBlock);

        var ct = active.Token;
        Task.Run(() => AdvanceRoundLoop(ct), ct);
    }

    public void Stop()
    {
        try
        {
            active.Cancel();
            active.Dispose();
            logger.LogInformation("Replica {nodeid} stopped constructing DAG", self.nodeid);
        }
        catch (Exception)
        {
            logger.LogWarning("Stopping process is not completed");
        }

    }


    /// <summary>
    /// Call this method to submit a new message to the DAG.
    /// </summary>
    /// <param name="msg"></param>
    public void SubmitMessage(UpdateMessage msg)
    {
        lock (updatesBuffer)
        {
            updatesBuffer.AddLast(msg);
        }

        Interlocked.Increment(ref stats.receivedUpdates);
        //logger.LogTrace("Submitted update message");
    }

    /// <summary>
    /// Get the current round of the DAG
    /// </summary>
    /// <returns></returns>
    public int GetCurRound()
    {
        return currentRoundState.round;
    }

    /// <summary>
    /// Get DAG stats
    /// </summary>
    /// <returns></returns>
    public Stats GetStats()
    {
        return stats.Clone() as Stats;
    }

    /// <summary>
    /// Return the state of the current round as a list of strings
    /// </summary>
    /// <returns> A list of strings
    /// [round, 
    /// hash of current round block
    /// # of received signature for the block,
    /// # of received current round blocks
    /// # of received current round certs, 
    /// if the node has reached consensus for current round
    /// ...]</returns>
    public string[] getCurrRoundState()
    {
        int currentRoundReceivedBlocks;

        try
        {
            currentRoundReceivedBlocks = receivedBlocks[currentRoundState.round].Count;
        }
        catch (Exception)
        {
            currentRoundReceivedBlocks = 0;
        }

        string hash;
        try
        {
            hash = System.Text.Encoding.Default.GetString(currentRoundState.currentRoundBlock.digest);
        }
        catch (Exception)
        {
            hash = "null";
        }

        int currentRoundUpdateCount = currentRoundState.currentRoundBlock is null ? 0 : currentRoundState.currentRoundBlock.updateMsgs.Count;

        return new string[] {   $"{currentRoundState.round}",
                                $"{hash}",
                                $"{currentRoundState.receivedAcks.Count}",
                                $"{currentRoundReceivedBlocks}",
                                $"{currentRoundState.didConsensus}", // TODO:
                                $"{currentRoundState.didConsensus}",
                                $"{currentRoundUpdateCount}"};
    }

    /// <summary>
    /// Check if any received blocks that are invalid, i.e does not have a certificate while its source
    /// is at a higher round
    /// </summary>
    public List<VertexBlock> PruneInvalidBlocks()
    {
        int pruneToRound = committee.atRounds.Values.Min();
        List<VertexBlock> results = new();
        HashSet<byte[]> referencedBlocks = new(new ByteArrayComparer());

        for (int i = pruneToRound - 1; i > lastPrunedRound; i--)
        {
            // check if every block in the round has at least referred by blocks in the next round once
            foreach (var cert in receivedCertificates[i + 1].Values)
            {
                foreach (var prev in blocksIndexedByHash[cert.blockHash].prevCertificates)
                {
                    referencedBlocks.Add(prev);
                }
            }

            // all blocks are referenced, no need to check
            if (referencedBlocks.Count == committee.numNodes)
            {
                referencedBlocks.Clear();
                continue;
            }
            else
            {
                foreach (var block in receivedBlocks[i].Values)
                {
                    if (!referencedBlocks.Contains(block.digest))
                    {
                        results.Add(block);
                    }
                }
                referencedBlocks.Clear();
            }
        }

        lastPrunedRound = pruneToRound;

        return results;
    }

    public void HandleMessage(DAGMessage msg)
    {
        // Stopwatch sw = new();
        // sw.Start();
        int type = -1;


        switch (msg)
        {
            case InitMessage initMessage:
                advandRoundLock.EnterReadLock();
                try
                {
                    ReceivedInit(initMessage);
                }
                finally
                {
                    advandRoundLock.ExitReadLock();
                }
                type = 0;
                break;
            case VertexBlockMessage vbmsg:
                advandRoundLock.EnterReadLock();
                try
                {
                    ReceivedBlock(vbmsg.GetBlock(), vbmsg.IsQueryReply);
                }
                finally
                {
                    advandRoundLock.ExitReadLock();
                }
                stats.receivedBlockMsgCount++;
                type = 1;
                break;
            case CertificateMessage certificateBlock:
                advandRoundLock.EnterUpgradeableReadLock();
                try
                {
                    ReceivedCertificate(certificateBlock.GetCertificate());
                }
                finally
                {
                    advandRoundLock.ExitUpgradeableReadLock();
                }
                stats.receivedCertMsgCount++;
                type = 2;
                break;
            case SignatureMessage signatureMessage:
                advandRoundLock.EnterReadLock();
                try
                {
                    ReceivedSignature(signatureMessage);
                }
                finally
                {
                    advandRoundLock.ExitReadLock();
                }
                stats.receivedAckMsgCount++;
                type = 3;
                break;
            case BlockQueryMessage blockQueryMessage:
                advandRoundLock.EnterReadLock();
                try
                {
                    ReceivedBlockQuery(blockQueryMessage);
                }
                finally
                {
                    advandRoundLock.ExitReadLock();
                }
                stats.receivedBlockQueryMsgCount++;
                type = 4;
                break;
            default:
                break;
        }


        // sw.Stop();
        // logger.LogTrace($"Handling Type {type} from {msg.source} for round {msg.round} took {sw.ElapsedMilliseconds} ms");
        // sw.Reset();
    }

    public void ReceivedInit(InitMessage msg)
    {
        // this needs to be synchronized to properly check if 
        // all replicas are initialized
        lock (this)
        {
            logger.LogTrace("Received init message from {s}", msg.source);

            if (isInit)
                throw new InvalidOperationException("Received init message after initialization");

            if (msg.source == self.nodeid)
                return;

            if (committee.GetNode(msg.source).publicKeyHandler is null)
                committee.GetNode(msg.source).publicKey = msg.publicKey;

            // check if all replicas are initialized by checking if replica field is not null
            if (committee.IsInitd())
            {
                isInit = true;
                logger.LogInformation("All replicas are initialized");
            }
        }
    }

    /// <summary>
    /// Handle received a broadcasted block
    /// </summary>
    /// <param name="block"></param>
    /// <param name="IsQueryReply"></param>
    public void ReceivedBlock(VertexBlock block, bool IsQueryReply)
    {
        string blockInfo = $"vertex block for round {block.round} from {block.source} with {block.updateMsgs.Count} updates";

        // check signature  
        if (!block.Verify(committee.GetNodeKey(block.source)))
        {
            logger.LogWarning("Reject {blockInfo} due to invalid signature", blockInfo);
            return;
        }

        // if the block is the return of a query for a block
        if (IsQueryReply)
        {
            // if already received the block
            if (GetBlocks(block.round).ContainsKey(block.source))
            {
                return;
            }

            AddBlock(block);

            blocksIndexedByHash[block.digest] = block;

            logger.LogTrace("Received queried {blockInfo}", blockInfo);
        }
        else
        {
            bool flag = true;

            // if block is not on the same round, still stores it but not acknowledged
            if (block.round < currentRoundState.round)
            {
                logger.LogTrace("Reject {blockInfo} due to wrong round, currentRound {r}", blockInfo, currentRoundState.round);
                flag = false;
            }

            // check if there is already a block from the replica in this round
            if (flag && block.round < receivedBlocks.Count && GetBlocks(block.round).ContainsKey(block.source))
            {
                logger.LogWarning("Reject {blockInfo} due to duplicate", blockInfo);
                return;
            }

            // if we are at round 0 or check if it has 2f + 1 certificate for previous round
            if (block.prevCertificates.Count < 2 * f + 1 && block.round != 0)
            {
                logger.LogWarning("Reject {blockInfo} due to insufficient certificates", blockInfo);
                return;
            }

            AddBlock(block);

            if (flag)
            {
                logger.LogTrace("Received {blockInfo}", blockInfo);
                AcknowledgeBlock(block);
            }
        }
    }

    /// <summary>
    /// Handle received a message querying for a block
    /// </summary>
    /// <param name="blockQueryMessage"></param>
    public void ReceivedBlockQuery(BlockQueryMessage blockQueryMessage)
    {
        if (!blocksIndexedByHash.TryGetValue(blockQueryMessage.blockHash, out VertexBlock block))
            return;

        msgSender.SendBlock(block, blockQueryMessage.source, true);
        stats.sentBlockMsgCount++;
        logger.LogTrace("Sending queried vertex block for round {r} to node {s}",
            blockQueryMessage.round,
            blockQueryMessage.source);
    }


    /// <summary>
    /// Handle received signature for broadcasted block
    /// </summary>
    /// <param name="signature"></param>
    public void ReceivedSignature(SignatureMessage signature)
    {
        byte[] blockHash = GetBlock(signature.round, self.nodeid).digest;

        // if the block already has certificate
        if (certificatesIndexedByHash.TryGetValue(blockHash, out _))
            return;

        if (signature.round != currentRoundState.round)
            return;

        if (!signature.Verify(blockHash, committee.GetNodeKey(signature.source)))
            return;

        logger.LogTrace("Received signature from {s} for block for round {r} current round {r}",
            signature.source,
            signature.round,
            currentRoundState.round);

        // add to received acks if source is not in the dict
        currentRoundState.receivedAcks.TryAdd(signature.source, signature.signature);

        Certificate certificate = null;
        lock (currentRoundState)
        {
            if (currentRoundState.receivedAcks.Count >= (2 * f) + 1 && currentRoundState.currentRoundBlock.certificate is null)
            {
                logger.LogTrace("Received enough acks for block at round {r}", currentRoundState.round);

                certificate = new(
                   currentRoundState.currentRoundBlock.round,
                   self.nodeid,
                   currentRoundState.currentRoundBlock.digest,
                   new Dictionary<int, byte[]>(currentRoundState.receivedAcks) // make a copy of the current received acks
               );

                // this is from myself, so no need to do additional check
                currentRoundState.currentRoundBlock.certificate = certificate;
                receivedCertificates[currentRoundState.round][self.nodeid] = certificate;
                bool check = certificatesIndexedByHash.TryAdd(certificate.blockHash, certificate);
                Debug.Assert(check, "Certificate already added");
                check = blocksIndexedByHash.TryAdd(certificate.blockHash, currentRoundState.currentRoundBlock);
                Debug.Assert(check, "Block already added");
            }
        }

        logger.LogTrace("Already Received {c} acks at round {r} ", currentRoundState.receivedAcks.Count, currentRoundState.round);

        if (certificate is not null)
        {
            int rnd = random.Next(0, 100);
            // log all variables in the if

            if (options.faultyRate == 0 || currentRoundState.round == 0 || rnd > options.faultyRate)
            {
                msgSender.BroadcastCertificate(certificate);
            } 
            else // simulate a faulty node
            {
                receivedCertificates[currentRoundState.round].TryRemove(self.nodeid, out _);
                certificatesIndexedByHash.TryRemove(certificate.blockHash, out _);
                blocksIndexedByHash.TryRemove(certificate.blockHash, out _);

                // ops that are not processed will not be counted
                Globals.perfCounter?.OpSubtract((ulong)currentRoundState.currentRoundBlock.updateMsgs.Count);
                logger.LogWarning("Faulty node, not sending certificate for round {r}", certificate.round);
            }

            committee.atRounds[self.nodeid] = currentRoundState.round;
            stats.sentCertMsgCount++;
            logger.LogTrace("Sent certificate for round {r}", certificate.round);
        }

    }

    /// <summary>
    /// Process a received certificate
    /// </summary>
    /// <param name="cert"></param>
    public void ReceivedCertificate(Certificate cert)
    {
        if (!cert.CheckSignatures(committee.nodeKeys))
        {
            stats.rejectByInvalidSignatureCount += 1;
            logger.LogWarning("Reject certificate for round {r} from {s} due to invalid signature", cert.round, cert.source);
            return;
        }

        lock (certificatesBuffer)
        {
            certificatesBuffer.Enqueue(cert, cert.round);


            logger.LogTrace("Received certificate from {s} for round {r}", cert.source, cert.round);


            // if block has not been received, query for it
            VertexBlock block = GetBlock(cert.round, cert.source);
            if (cert.round >= receivedBlocks.Count || block == null)
            {
                logger.LogTrace($"The block of the certificate has not been received, querying");
                QueryForBlock(cert);
            }
            else
            {
                block.certificate = cert;
            }

            // this is ok, if the node is lying for being at a higher round, there are enough correct nodes
            // if the node is lying for being at a lower round, it will be recognized by the (supposedly) timeout check
            committee.atRounds[cert.source] = Math.Max(committee.atRounds[cert.source], cert.round);

            CheckCertificates();
        }
    }

    // Query for a block if a certificate is received but the block is not
    private void QueryForBlock(Certificate certificateBlock)
    {
        logger.LogTrace("Querying for vertex block from node {s} at round {r}",
            certificateBlock.source,
            certificateBlock.round);

        // get list of nodes that have the block and store in an array
        msgSender.QueryForBlock(certificateBlock.blockHash, committee.NodeIds());
        stats.sentBlockQueryMsgCount++;
    }


    // temporary hold certificates while checking for predecessors
    readonly List<Certificate> certificateCheckTempList = new();
    /// <summary>
    /// Check buffered certificates, if all it's predecessors are received.
    /// </summary>
    public void CheckCertificates()
    {
        certificateCheckTempList.Clear();

        if (certificatesBuffer.Count == 0)
            stats.noCertificateCheckCount++;

        while (certificatesBuffer.Count > 0)
        {
            var cert = certificatesBuffer.Dequeue();

            // if the corresponding Vertexblock is not received, 
            var block = GetBlock(cert.round, cert.source);
            if (cert.round >= receivedBlocks.Count || block is null)
            {
                logger.LogTrace("Block for round {r} from {s} has not been received", cert.round, cert.source);
                // put it back to the buffer
                certificateCheckTempList.Add(cert);
                continue;
            }

            bool missingPrev = false;
            // check if all predecessors certificates are received
            foreach (var prev in block.prevCertificates)
            {
                // if one of the predecessor is not received, put it back to the buffer
                if (!certificatesIndexedByHash.ContainsKey(prev))
                {
                    // print the hash of the predecessor
                    logger.LogTrace("Certificate for round {r} from {s} has missing predecessor", cert.round, cert.source);
                    stats.missingPredecessorCount++;

                    certificateCheckTempList.Add(cert);
                    missingPrev = true;
                    break;
                }
            }

            if (missingPrev)
                continue;

            // index block and certificate using block hash
            // block is only indexed if it has a certificate
            // block and certificate should only be added once
            block.certificate ??= cert;

            bool success;
            if (cert.source != self.nodeid)
            {
                success = blocksIndexedByHash.TryAdd(cert.blockHash, block);
                if (!success)
                    logger.LogDebug("block of round {r} from {s} already added", cert.round, cert.source);
                Debug.Assert(success);
            }

            success = certificatesIndexedByHash.TryAdd(cert.blockHash, cert);
            AddRoundsUntil(receivedCertificates, cert.round);
            receivedCertificates[cert.round][cert.source] = cert;
            if (!success)
                logger.LogDebug("cert of round {r} from {s} already added", cert.round, cert.source);
            Debug.Assert(success);

            if (receivedBlockEvent is not null)
                receivedBlockEvent(block);

        }

        // add remaining certificates back to the buffer
        foreach (var cert in certificateCheckTempList)
            certificatesBuffer.Enqueue(cert, cert.round);

        logger.LogTrace("Checking to see if there are enough certificates to advance round");
        // going from the last rounds with received certificates, check if we can advance round
        for (int i = receivedCertificates.Count - 1; i >= currentRoundState.round; i--)
        {
            logger.LogTrace("Received {count} certificates for round {r}", receivedCertificates[i].Count, i);
            // if the round has received enough certificates
            if (receivedCertificates[i].Count >= 2 * f + 1)
            {
                currentRoundState.enoughCertForRoundi = i;
                currentRoundState.canAdvanceRoundNotifier.Set();
                logger.LogTrace("Enough certificates for round {r}", i);
                break;
            }
        }
    }


    /// <summary>
    /// An async thread continuously check if the round can be advanced
    /// </summary>
    private void AdvanceRoundLoop(CancellationToken cancellationToken)
    {
        blockCreationTimer.Restart();
        while (true)
        { 
            int signaled = WaitHandle.WaitAny(new[] {currentRoundState.canAdvanceRoundNotifier, cancellationToken.WaitHandle} );
            
            if (signaled == 1)
                return;

            int round = currentRoundState.enoughCertForRoundi;

            if (round < currentRoundState.round || receivedCertificates[round].Count < 2 * f + 1)
            { 
                continue;
            }
                
            if (updatesBuffer.Count < options.maxBatchSize && blockCreationTimer.ElapsedMilliseconds < options.blockInterval && round == currentRoundState.round)
            {
                logger.LogTrace("Waiting for block interval, only {ms} ms passed", blockCreationTimer.ElapsedMilliseconds);
                Thread.Sleep(options.blockInterval - (int)blockCreationTimer.ElapsedMilliseconds);
            }
            advandRoundLock.EnterWriteLock();
            try
            {
                stats.callingAdvanceRoundCount += 1;

                VertexBlock block;
                List<UpdateMessage> updatesList = null;

                logger.LogTrace("Advancing round now");

                lock (updatesBuffer)
                {
                    // discard previous round's blocks if it does not received enough acks
                    // ofc round 0 has no previous round
                    if (currentRoundState.round > 0 && currentRoundState.currentRoundBlock != null && currentRoundState.currentRoundBlock.certificate == null && options.faultyRate == 0)
                    {
                        logger.LogTrace("Discarding block at round {r}", currentRoundState.round);
                        // to make sure the original order of updates are persevered
                        LinkedListNode<UpdateMessage> lastmsg = null;
                        foreach (var msg in currentRoundState.currentRoundBlock.updateMsgs)
                        {
                            if (lastmsg is null)
                                lastmsg = updatesBuffer.AddFirst(msg);
                            else
                            {
                                updatesBuffer.AddAfter(lastmsg, msg);
                                lastmsg = lastmsg.Next;
                            }
                        }
                    }


                    updatesList = new List<UpdateMessage>(options.maxBatchSize);
                    int count = 0;
                    while (updatesBuffer.Count > 0 && count < options.maxBatchSize)
                    {
                        updatesList.Add(updatesBuffer.First.Value);
                        updatesBuffer.RemoveFirst();
                        count++;
                    }

                    // reset current round state
                    currentRoundState.ToRound(round + 1);
                }

                // if receivedBlocks do not have current round yet
                AddRoundsUntil(receivedBlocks, currentRoundState.round);
                AddRoundsUntil(receivedCertificates, currentRoundState.round);

                logger.LogTrace("Advanced to round {r}", currentRoundState.round);

                if (!currentRoundState.didConsensus &&
                    consensus is not null &&
                    consensus.isLastRoundOfWave(currentRoundState.round) &&
                    currentRoundState.round != 0)
                {
                    consensus.GetWave(currentRoundState.round, out int wave);
                    // compute time to do this function
                    currentRoundState.didConsensus = true;
                    consensus.Commit(wave);
                    stats.numConsensusCompleted++;
                }

                // create and broadcast a new block
                List<byte[]> prevCertificates = new();
                receivedCertificates[currentRoundState.round - 1].Values.ToList().ForEach(b => prevCertificates.Add(b.blockHash));
                block = CreateBlock(prevCertificates, updatesList);
                currentRoundState.currentRoundBlock = block;

                msgSender.BroadcastBlock(block);
                logger.LogTrace("Broadcasted block at round {r} with {count} updates", block.round, block.updateMsgs.Count);
                stats.sentBlockMsgCount += 1;
                blockCreationTimer.Restart();
            }
            finally
            {
                advandRoundLock.ExitWriteLock();
            }
        }

    }


    // get a block from receivedBlocks, thread safe
    public VertexBlock GetBlock(int round, int nodeid)
    {
        bool lockTaken = false;

        try
        {
            spinLock0.Enter(ref lockTaken);

            if (round < receivedBlocks.Count)
            {
                if (receivedBlocks[round].TryGetValue(nodeid, out VertexBlock b))
                    return b;
            }

            return null;
        }
        finally
        {
            if (lockTaken)
                spinLock0.Exit();
        }
    }

    public Certificate GetCertificate(int round, int nodeid)
    {
        bool lockTaken = false;

        try
        {
            spinLock0.Enter(ref lockTaken);

            if (round < receivedCertificates.Count)
            {
                if (receivedCertificates[round].TryGetValue(nodeid, out Certificate c))
                    return c;
            }

            return null;
        }
        finally
        {
            if (lockTaken)
                spinLock0.Exit();
        }
    }

    // get all blocks of a round, thread safe
    public ConcurrentDictionary<int, VertexBlock> GetBlocks(int round)
    {
        bool lockTaken = false;

        try
        {
            spinLock0.Enter(ref lockTaken);

            if (round < receivedBlocks.Count)
            {
                return receivedBlocks[round];
            }

            return null;
        }
        finally
        {
            if (lockTaken)
                spinLock0.Exit();
        }
    }

    // create a new block
    private VertexBlock CreateBlock(List<byte[]> prevCertificates, List<UpdateMessage> updatesList)
    {
        VertexBlock block = new(currentRoundState.round, self.nodeid, prevCertificates, updatesList);
        block.Sign(self.privateKey);
        receivedBlocks[currentRoundState.round].TryAdd(self.nodeid, block);
        //blocksIndexedByHash[block.digest] = block;
        currentRoundState.currentRoundBlock = block;
        currentRoundState.receivedAcks.TryAdd(self.nodeid, block.signature);

        return block;
    }

    // add a block to receivedBlocks, thread safe
    private void AddBlock(VertexBlock block)
    {
        bool lockTaken = false;

        try
        {
            spinLock0.Enter(ref lockTaken);
            AddRoundsUntil(receivedBlocks, block.round);

            receivedBlocks[block.round][block.source] = block;
        }
        finally
        {
            if (lockTaken)
                spinLock0.Exit();
        }
    }

    // add new dictionaries to receivedBlocks or receivedCertificates until the round is there
    private void AddRoundsUntil<T>(List<ConcurrentDictionary<int, T>> blockList, int until) where T : Block
    {
        while (blockList.Count <= until)
            blockList.Add(new ConcurrentDictionary<int, T>(committee.numNodes, committee.numNodes));
    }


    // create a signature for the block
    private void AcknowledgeBlock(VertexBlock block)
    {
        byte[] sig = block.ComputeSignature(self.privateKey);
        msgSender.SendSignature(block.round, sig, block.source);
        stats.sentAckMsgCount++;
    }

    /// <summary>
    /// Garbage collect the DAG by removing blocks that are already committed by all replicas
    /// </summary>
    private void GarbageCollect()
    {
        // garbage collect blocks that are already committed by all replicas (assume they are already persisted)
        int collectToRound = Math.Min(consensus.GetRound(consensus.lastCommittedWave), committee.atRounds.Values.Max());
        for (int i = lastCollectedRound; i < collectToRound; i++)
        {
            if (receivedBlocks[i].Count >= committee.numNodes)
            {
                foreach (var block in receivedBlocks[i].Values)
                {
                    if (block.certificate is not null && block.certificate.CheckSignatures(committee.nodeKeys))
                    {
                        receivedBlocks[i].TryRemove(block.source, out _);
                        blocksIndexedByHash.TryRemove(block.digest, out _);
                    }
                }
            }
        }
        lastCollectedRound = collectToRound;
    }

}