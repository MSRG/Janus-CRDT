using System;
using System.Threading;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;

public class ORSetCommand : CRDTCommand
{
    public ORSetCommand(string key, string typeCode, string opCode, bool isSafe, string[] paramsList, ILogger logger) : base(key, typeCode, opCode, isSafe, paramsList, logger) { }

    public override bool Execute(KeySpaceManager ksm, (Connection, uint) origin, out string output)
    {
        string key = this.key;
        output = "";
        bool res;

        try
        {
            if (opCode == "s")
            {
                ksm.CreateNewKVPair<ORSet<string>>(key);
                return true;
            }
            else
            {
                if (ksm.GetKVPair(key, out SafeCRDT instance))
                {
                    lock (instance)
                    {

                        if (opCode == "gp")
                        {
                            output = instance.QueryProspective(new object[] { paramsList[0] }).ToString();
                            res = true;
                        }
                        else if (opCode == "gs")
                        {
                            output = instance.QueryStable(new object[] { paramsList[0] }).ToString();
                            res = true;
                        }
                        else if (opCode == "a")
                        {
                            res = (bool)instance.Update(1, new object[] { paramsList[0] }, isSafe, origin);
                            if (isSafe)
                            {
                                //Interlocked.Increment(ref Globals.perfCounter.recievedSafeUpdateCount);
                                output = "su";
                            }
                        }
                        else if (opCode == "r")
                        {
                            res = (bool)instance.Update(2, new object[] { paramsList[0] }, isSafe, origin);
                            if (isSafe)
                                output = "su";
                        }
                        else if (opCode == "c")
                        {
                            res = (bool)instance.Update(3, null, false, origin);
                        }
                        else
                        {
                            logger.LogWarning("Unknown command: " + this.ToString());
                            res = false;
                        }

                        return res;
                    }
                }
                else
                {
                    output = "Key not found";
                    return false;
                }
            }
        }
        catch (Exception e)
        {
            var error = $"Error in handling commands: {e.Message} at {e.StackTrace}";
            logger.LogError(error);
            output = error;
            return false;
        }


    }
}
