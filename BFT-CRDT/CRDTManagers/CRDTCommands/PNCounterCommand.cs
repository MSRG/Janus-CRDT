using System;
using MergeSharp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;

public class PNCounterCommand : CRDTCommand
{
    public PNCounterCommand(string key, string typeCode, string opCode, bool isSafe, string[] paramsList, ILogger logger) : base(key, typeCode, opCode, isSafe, paramsList, logger) { }

    public override bool Execute(KeySpaceManager ksm, (Connection, uint) origin, out string output)
    {
        string key = this.key;
        output = "";
        bool res;

        try
        {
            if (opCode == "s")
            {
                ksm.CreateNewKVPair<PNCounter>(key);
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
                            output = instance.QueryProspective().ToString();
                            res = true;
                        }
                        else if (opCode == "gs")
                        {
                            output = instance.QueryStable().ToString();
                            res = true;
                        }
                        else if (opCode == "i")
                        {
                            res = (bool)instance.Update(1, new object[] { int.Parse(paramsList[0]) }, isSafe, origin);
                            if (isSafe)
                                output = "su";
                        }
                        else if (opCode == "d")
                        {
                            res = (bool)instance.Update(1, new object[] { int.Parse(paramsList[0]) }, isSafe, origin);
                            if (isSafe)
                                output = "su";
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
