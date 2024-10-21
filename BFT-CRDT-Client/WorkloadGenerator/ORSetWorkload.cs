using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BFTCRDT.Client;

public class ORSetWorkloadGenerator : WorkloadGenerator
{

    private int addCount = 0;
    private int removeCount = 0;
    private float addRatio;
    private float rmvRatio;
    private float readRatio;

    private ConcurrentDictionary<string, ConcurrentBag<string>> added;

    public ORSetWorkloadGenerator(string typeCode, int numObjs, float[] opsRatios, float safeRatio) : base(typeCode, numObjs, opsRatios, safeRatio)
    {
        if (opsRatios.Length != 3)
        {
            throw new ArgumentException("Invalid number of arguments for PNC benchmark");
        }

        this.addRatio = opsRatios[0];
        this.rmvRatio = opsRatios[1];
        this.readRatio = opsRatios[2];

        added = new();

    }


    public override ClientMessage GetNextCommand(int index = -1)
    {

        int maxkey = (index + 1) * 8;
        if (index == 11)
        {
            maxkey = 99;
        }


        string key = keys[keyLoc];
        sequenceNumber++;

        if (added[key].Count == 50)
        {
            added[key].Clear();
            return new ClientMessage() 
            { 
                sourceType = SourceType.Client,
                sequenceNumber = sequenceNumber,
                key = key, 
                typeCode = typeCode, 
                opCode = "c", 
                isSafe = false, 
                paramsList = new string[] { } 
            };
        }


        string op;
        switch(PickRandomOptionByRatio())
        {
            case 0: 
            case 1:
                op = "a";
                addCount++;
                break;
            case 2:
                op = "g";
                break;
            default:
                throw new Exception("Invalid option");
        }

        if (added[key].Count == 0)
        {
            op = "a";
        }
        

        if (op == "a" )
        {
            string str = GenerateRandomString(5);
            added[key].Add(str);

            return new ClientMessage() 
            { 
                sourceType = SourceType.Client,
                sequenceNumber = sequenceNumber,
                key = key, 
                typeCode = typeCode, 
                opCode = op, 
                isSafe = IsSafe(), 
                paramsList = new string[] { str } 
            };
        }
        else if (op == "g")
        {

            try
            {
                int i = random.Next(added[key].Count);
                string toCheck = added[key].ElementAt(i);
                return new ClientMessage() 
                { 
                    sourceType = SourceType.Client,
                    sequenceNumber = sequenceNumber,
                    key = key, 
                    typeCode = typeCode, 
                    opCode = "gp", 
                    isSafe = IsSafe(), 
                    paramsList = new string[] { toCheck } 
                };
            }
            catch (ArgumentOutOfRangeException)
            {
                string str = GenerateRandomString(5);
                added[key].Add(str);

                return new ClientMessage() 
                { 
                    sourceType = SourceType.Client,
                    sequenceNumber = sequenceNumber,
                    key = key, 
                    typeCode = typeCode, 
                    opCode = op, 
                    isSafe = IsSafe(), 
                    paramsList = new string[] { str } 
                };
            }
            
        }   

        keyLoc++;
        if (keyLoc > maxkey)
        {
            keyLoc = 0;
        }

        return null;
    }

    public override ClientMessage CheckObjectExistCommand(string key)
    {
        sequenceNumber++;

        foreach (var item in keys)
        {
            added[item] = new ConcurrentBag<string>();
        }
        

        return new ClientMessage() 
        { 
            sourceType = SourceType.Client,
            sequenceNumber = sequenceNumber,
            key = key, 
            typeCode = typeCode, 
            opCode = "gp", 
            isSafe = false,     
            paramsList = new string[] { "" } 
        };
    }

    /// <summary>
    /// True for get, false for update
    /// </summary>
    /// <param name="opCode"></param>
    /// <returns></returns>
    public override bool IsGetorUpdate(string opCode)
    {
        if (opCode == "g" || opCode == "gp" || opCode == "gs" || opCode == "c")
        {
            return true;
        }
        else if (opCode == "a" || opCode == "r")
        {
            return false;
        }
        else
        {
            throw new ArgumentException($"Invalid opCode:{opCode}");
        }
        
    }
}