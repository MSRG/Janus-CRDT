using System;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;

public class StatsCommand : CRDTCommand
{
    public StatsCommand(string key, string typeCode, string opCode, bool isSafe, string[] paramsList, ILogger logger = null) : base(key, typeCode, opCode, isSafe, paramsList, logger)
    {
    }

    public override bool Execute(KeySpaceManager ksm, (Connection, uint) origin, out string output)
    {
        //var res = ksm.GetDagStats();
        var res = Globals.perfCounter.GetReport(out ulong totalops);
        // convert array of string to a single string
        output = totalops.ToString() + ":" + string.Join(",", res);
        return true;
    }
}