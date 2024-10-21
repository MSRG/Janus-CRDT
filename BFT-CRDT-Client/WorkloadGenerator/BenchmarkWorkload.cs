using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BFTCRDT.Client;


public abstract class WorkloadGenerator
{

    protected string typeCode;
    protected string[] keys;
    protected float[] opsRatio;
    protected float safeRatio;
    protected int keyLoc;
    protected Random random;
    public uint sequenceNumber = 0;

    public WorkloadGenerator(string typeCode, int numObjs, float[] opsRatios, float safeRatio)
    {
        this.typeCode = typeCode;
        this.keys = new string[numObjs];
        this.opsRatio = opsRatios;
        this.safeRatio = safeRatio;

        this.random = new();
        // start with a random key 
        this.keyLoc = random.Next(keys.Length);
    }
    
    public abstract ClientMessage GetNextCommand(int index = -1);
    public abstract ClientMessage CheckObjectExistCommand(string key);

    public abstract bool IsGetorUpdate(string opCode);

    public void ResetKeyLoc()
    {
        keyLoc = random.Next(keys.Length);
    }

    /// <summary>
    /// Pick a random index based on the ratios of opsRatios
    /// </summary>
    /// <returns>The index of the selected ratio</returns>
    public int PickRandomOptionByRatio()
    {
        float rand = random.NextSingle();
        float sum = 0;
        for (int i = 0; i < opsRatio.Length; i++)
        {
            sum += opsRatio[i];
            if (rand < sum)
            {
                return i;
            }
        }

        // If we get here, something went wrong with the ratios
        throw new InvalidOperationException("Invalid ratios for PickRandomOptionByRatio");
    }

    /// <summary>
    /// Based on the safeRatio, decide if the operation is safe or not
    /// </summary>
    /// <returns>true for safe, false for unsafe</returns>
    protected bool IsSafe()
    {
        return random.NextSingle() < safeRatio;
    }

    public string[] InitObj(ServerConnection serverConnection)
    {
        
        // generate 100 unique strings with length of 5, with 0-9, a-z, A-Z
        for (int i = 0; i < keys.Count(); i++)
        {
            keys[i] = GenerateRandomString(5);
            var msg = new ClientMessage()
            {
                sourceType = SourceType.Client,
                key = keys[i],
                typeCode = typeCode,
                opCode = "s",
                isSafe = false,
                paramsList = new string[] { }
            };
            serverConnection.Transmit(msg);
        }

        return keys;
    }

    private volatile int CreatedCount;
    private volatile bool error;

    public bool VerifyCreate(ServerConnection serverConnection)
    {
        CreatedCount = 0;
        error = false;

        serverConnection.StartReceiveResult(NotifyCreatedCallback, null);

        foreach (string k in keys)
        {
            var msg = CheckObjectExistCommand(k);
            serverConnection.Transmit(msg);
        }
        
        int i = 0;
        
        while (CreatedCount < keys.Length && !error)
        {
            // timeout
            if (i >= 100)  
            {
                Console.WriteLine("Timeout getting create result");
                error = true;
                break;
            }

            Thread.Sleep(100);
            i++;
        }

        serverConnection.StopReceiveResult();
        return !error;

       
    }

    private void NotifyCreatedCallback(ClientMessage msg, Result result)
    {
        if (msg.opCode == "gp" && keys.Contains(msg.key))
        {
            if (msg.result)
                CreatedCount++;
            else
                error = true;
        }

    }





    protected string GenerateRandomString(int length)
    {
        const string chars = "abcdefghijklmnorqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
          .Select(s => s[random.Next(s.Length)]).ToArray());
    }


    public object ShallowCopy()
    {   
        random = new Random();
        return this.MemberwiseClone();
    }

}

