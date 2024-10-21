using System;
using System.Collections.Generic;
using BFTCRDT.DAG;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BFTCRDT;


// TODO: this very bad, need refactor later
public static class Globals
{
    public static ILogger logger =  new NullLogger<Program>();
    public static PerfCounter perfCounter;

    public static void LoggerSetup(string loggingLevel, int nodeid)
    {
        ILoggerFactory loggerFactory = null;

        if (loggingLevel == "-1")
        {
            logger =  new NullLogger<Program>();
        }
        else
        {
            loggerFactory = LoggerFactory.Create(builder =>
            {
                LogLevel logLevel;
                switch (loggingLevel)
                {
                    case "0": case "1": case "2": case "3": case "4": case "5": case "6":
                        logLevel = Enum.Parse<LogLevel>(loggingLevel);
                        break;
                    default:
                        logLevel = LogLevel.Debug;
                        break;
                }
                
                builder.AddConsole(c => c.TimestampFormat = "[HH:mm:ss]").SetMinimumLevel(logLevel);
                //builder.AddConsole();


            });
            logger = loggerFactory.CreateLogger("Logger");
        }

        if (loggerFactory is not null)
            logger = loggerFactory.CreateLogger($"Node {nodeid}");
    }

    public static void Init()
    {   
        if (logger is null)
            Console.WriteLine("Warning: logger is not initialized");

        perfCounter = new PerfCounter(logger);
    }
}


// public class ConfigFileX
// {
//     public ILogger logger { get; set; }
//     public ConfigParser cluster { get; set; }
//     private string clusterInfoFile;

//     public ConfigFileX(string clusterInfoFile, string loggingLevel)
//     {

//         ILoggerFactory loggerFactory = null;

//         if (loggingLevel == "-1")
//         {
//             logger =  new NullLogger<ConfigFileX>();
//         }
//         else
//         {
//             loggerFactory = LoggerFactory.Create(builder =>
//             {
//                 LogLevel logLevel;
//                 switch (loggingLevel)
//                 {
//                     case "0": case "1": case "2": case "3": case "4": case "5": case "6":
//                         logLevel = Enum.Parse<LogLevel>(loggingLevel);
//                         break;
//                     default:
//                         logLevel = LogLevel.Debug;
//                         break;
//                 }
                
//                 builder.AddConsole().SetMinimumLevel(logLevel);
//                 builder.AddConsole();

//             });
//             logger = loggerFactory.CreateLogger("Logger");
//         }


//         // this.clusterInfoFile = clusterInfoFile;
//         // this.cluster = new ConfigParser(logger);
//         // this.cluster.ParseConfigJson(clusterInfoFile);

//         // if (loggerFactory is not null)
//         //     logger = loggerFactory.CreateLogger($"Node {cluster.selfNode.nodeid}");
//     }

// }
