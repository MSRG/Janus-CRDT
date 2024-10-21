using System;
using System.Collections.Generic;
using System.Linq;
using BFTCRDT.DAG;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ProtoBuf;

namespace BFTCRDT;

public class JanusService
{   
    private Cluster cluster;
    private KeySpaceManager ksm;
    private SafeCRDTManager sm;
    private ConnectionManager cm;
    private ReplicationManager rm;
    private DAG.DAG dag;

    private ClientInterface ci;
    
    private ConfigParser config;
    private ILogger logger;

    private bool isInit = false;
    
    bool ifCollectPerf = true;
    int clientBatchSize = 1000;

    public JanusService(string configFile)
    {
        this.config = new(configFile);
    }

    public void Init(string loggingLevel)
    {
        Console.WriteLine("Initializing services");

        Globals.LoggerSetup(loggingLevel, config.selfNode.nodeid);
        Globals.Init();
        logger = Globals.logger;

        Serializer.PrepareSerializer<ClientMessage>();
        Serializer.PrepareSerializer<InitMessage>();
        Serializer.PrepareSerializer<CertificateMessage>();
        Serializer.PrepareSerializer<VertexBlockMessage>();
        Serializer.PrepareSerializer<SignatureMessage>();
        Serializer.PrepareSerializer<BlockQueryMessage>();


        this.cluster = config.GetCluster(logger);

        CreateDAG();

        this.cm = new ConnectionManager(cluster, dag, logger);
        this.rm = new ReplicationManager(cm, null, logger);
        this.sm = new(cm, rm, logger)
        {
            clientBatchSize = clientBatchSize
        };

        cm.StartDAG();
        
        this.ksm = new(config.selfNode.nodeid, config.isPrimary(), sm, rm, logger);
        dag.consensus.consensusCompleteEvent += ksm.safeCRDTManager.HandleAfterConsensusUpdates;
        ksm.InitializeKeySpace();

        this.ci = new(cluster.self.ip, cluster.self.port + 1000, ksm, logger);

        isInit = true;
    }

    public void Start()
    {
        if (isInit)
        {
            logger.LogInformation("Staring up services");
            ci.Run();
            if (ifCollectPerf)
                Globals.perfCounter.StartReport();
        }
        else
        {
            Console.Error.WriteLine("Services not initialized");
        }
    }

    public void Stop()
    {
        try 
        {
            ci.Stop();
            ksm.Stop();
            Globals.perfCounter.StopReport();
        }
        finally 
        {
            Console.Error.WriteLine("Stopping services");
        }
    }


    private void CreateDAG()
    {
        List<Replica> replicas = Replica.GenerateReplicas(cluster.nodes.Keys.ToList(), cluster.self.nodeid, out Replica selfReplica);
        DAGMsgSender msgSender = new(cluster.self.nodeid, cluster.GetNodesList(), logger);
        this.dag = new(selfReplica, replicas, msgSender, logger);

        Consensus consensus = new(dag, logger);
        dag.consensus = consensus;
    }
}