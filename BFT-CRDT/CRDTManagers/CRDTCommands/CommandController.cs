using System;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;

public static class CommandFactory
{

    public static ILogger logger = null;

    public static CRDTCommand CreateCommand(string key, string typeCode, string opCode, bool isSafe, string[] paramsList)
    {
        switch (typeCode)
        {
            case "pnc":
                return new PNCounterCommand(key, typeCode, opCode, isSafe, paramsList, logger);
            case "orset":
                return new ORSetCommand(key, typeCode, opCode, isSafe, paramsList, logger);
            case "stats":
                return new StatsCommand(key, typeCode, opCode, isSafe, paramsList, logger);
            default:
                throw new Exception("Unknown CRDT type code " + typeCode);
        }
    }
}

public abstract class CRDTCommand
{
    protected string key;
    protected string typeCode;
    protected string opCode;
    protected string[] paramsList;
    protected ILogger logger;
    protected bool isSafe;


    public CRDTCommand(string key, string typeCode, string opCode, bool isSafe, string[] paramsList, ILogger logger = null)
    {
        this.key = key;
        this.typeCode = typeCode;
        this.opCode = opCode;
        this.paramsList = paramsList;
        this.logger = logger ?? NullLogger.Instance;
        this.isSafe = isSafe;
    }

    public abstract bool Execute(KeySpaceManager ksm, (Connection, uint) origin, out string output);
}
