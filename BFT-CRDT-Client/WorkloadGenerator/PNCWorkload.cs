using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BFTCRDT.Client;


public class PNCWorkloadGenerator : WorkloadGenerator
{

    private int incCount = 0;
    private int decCount = 0;
    private float incRatio;
    private float decRatio;
    private float readRatio;

    public PNCWorkloadGenerator(string typeCode, int numObjs, float[] opsRatios, float safeRatio) : base(typeCode, numObjs, opsRatios, safeRatio)
    {
        if (opsRatios.Length != 3)
        {
            throw new ArgumentException("Invalid number of arguments for PNC benchmark");
        }

        this.incRatio = opsRatios[0];
        this.decRatio = opsRatios[1];
        this.readRatio = opsRatios[2];

    }

    public override ClientMessage GetNextCommand(int index = -1)
    {
        string key = keys[keyLoc];
        sequenceNumber++;

        keyLoc++;
        if (keyLoc >= keys.Length)
        {
            keyLoc = 0;
        }

        string op;
        switch(PickRandomOptionByRatio())
        {
            case 0:
                op = "i";
                incCount++;
                break;
            case 1:
                op = "d";
                decCount++;
                break;
            case 2:
                op = "g";
                break;
            default:
                throw new Exception("Invalid option");
        }

        if (op == "i" || op == "d")
        {
            int amount = random.Next(1, 100);
            return new ClientMessage() 
            { 
                sourceType = SourceType.Client,
                sequenceNumber = sequenceNumber,
                key = key, 
                typeCode = typeCode, 
                opCode = op, 
                isSafe = IsSafe(), 
                paramsList = new string[] { amount.ToString() } 
            };
        }
        else if (op == "g")
        {
            return new ClientMessage() 
            { 
                sourceType = SourceType.Client,
                sequenceNumber = sequenceNumber,
                key = key, 
                typeCode = typeCode, 
                opCode = "gp", 
                isSafe = IsSafe(), 
                paramsList = new string[] { } 
            };
        }   

        

        return null;
    }

    public override ClientMessage CheckObjectExistCommand(string key)
    {
        sequenceNumber++;

        return new ClientMessage() 
        { 
            sourceType = SourceType.Client,
            sequenceNumber = sequenceNumber,
            key = key, 
            typeCode = typeCode, 
            opCode = "gp", 
            isSafe = false,     
            paramsList = new string[] { } 
        };
    }

    /// <summary>
    /// True for get, false for update
    /// </summary>
    /// <param name="opCode"></param>
    /// <returns></returns>
    public override bool IsGetorUpdate(string opCode)
    {
        if (opCode == "g" || opCode == "gp" || opCode == "gs")
        {
            return true;
        }
        else if (opCode == "i" || opCode == "d")
        {
            return false;
        }
        else
        {
            throw new ArgumentException($"Invalid opCode:{opCode}");
        }
        
    }



}