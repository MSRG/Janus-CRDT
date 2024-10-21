using System;
using System.Net;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace BFTCRDT;

class Program
{
    static int Main(string[] args)
    {
        string helpStr = @"Usage: KVDB <cluster_info_file> <logging_level>
Cluster Config File: See cluster_config_sample.json
Logging Level: Debug = 1, Information = 2, Warning = 3, Error = 4, Critical = 5, None = 6;
";

        string clusterFile;
        string debugLevel;

        if (args.Length < 2)
        {
            Console.WriteLine(helpStr);
            return 0;
        }

        clusterFile = args[0];
        debugLevel = args[1];


        JanusService mainService = new(clusterFile);

        try
        {
            mainService.Init(debugLevel);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine("Failed to initialize services, because of {0} at {1}", e.Message, e.StackTrace);
            mainService.Stop();
            return 0;
        }

        
        mainService.Start();

        Console.WriteLine("Press CTRL + C to exit");

        bool run = true;

        Console.CancelKeyPress += (sender, e) =>
        {
            // Handle the Ctrl+C event here
            Console.WriteLine("SIGINT received. Exiting...");
            // Perform any necessary cleanup or termination logic
            run = false;

            // Optionally, you can prevent the application from terminating immediately
            e.Cancel = true;
        };

        while (run)
            Thread.Sleep(1000);

        mainService.Stop();
        
        
        return 0;

    }



}

