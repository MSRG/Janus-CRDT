
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using BFTCRDT;

namespace BFTCRDT.Client;

public static class CommandParser
{
    public static Dictionary<string, string[]> TypeAndOps = new()
    {
        { "pnc", new[] { "s", "i", "d", "gp", "gs" } },
        { "orset", new[] { "s", "a", "r", "c", "gp", "gs" } },
    };



    public static ClientMessage Parse(string input)
    {
        
        string[] inputArray = input.Split(' ');

        if (inputArray.Length < 3 )
        {
            throw new ArgumentException("Invalid command format");
        }

        var typeCode = inputArray[0];
        var key = inputArray[1];
        var opCode = inputArray[2];
        
        bool isSafe = false;
        if (inputArray[3] == "y")
            isSafe = true;
        else if (inputArray[3] == "n")
            isSafe = false;


        if (!TypeAndOps.TryGetValue(typeCode, out var ops))
        {
            throw new ArgumentException("Invalid type code");
        }

        if (!ops.Contains(opCode))
        {
            throw new ArgumentException("Invalid operation code");
        }

        string[] paramsList = null;
        if (inputArray.Length > 4)
        {
            paramsList = inputArray[4..];
        }

        return new ClientMessage() 
        {
            sourceType = SourceType.Client,
            key = key,
            typeCode = typeCode,
            opCode = opCode,
            isSafe = isSafe,
            paramsList = paramsList
        };
        
       
    }
}