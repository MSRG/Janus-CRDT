using System;
using System.Net;
using Microsoft.Extensions.Logging;

namespace BFTCRDT.Client;

class Program
{
    static int Main(string[] args)
    {
        string helpStr = @"Usage: client <mode> (<ip> <port> | <benchmark config file> <oneshot?>)
mode = 1: interactive mode
mode = 2: benchmark mode        
oneshot = y: run benchmark once and exit
oneshot = n: run benchmark continuously
";
        string mode;
        try
        {
            mode = args[0];
        }
        catch (IndexOutOfRangeException)
        {
            Console.WriteLine(helpStr);
            return 0;
        }

        if (mode == "1")
        {
            string ip;
            string port;

            try
            {   
                ip = args[1];
                port = args[2];
            }
            catch (IndexOutOfRangeException)
            {
                Console.WriteLine(helpStr);
                return 0;
            }

            ServerConnection serverConnection = new(ip, Int32.Parse(port));
            
            if (!serverConnection.Connect())
                return 0;


            CommandLineInterface cmdInterface = new(serverConnection);
            cmdInterface.Start();

            serverConnection.Close();
        }
        else if (mode == "2")
        {
            string filename;
            bool oneshot = false;
            try
            {
                filename = args[1];
                if (args.Length == 3)
                {
                    oneshot = args[2] == "y";
                }
            }
            catch (IndexOutOfRangeException)
            {
                Console.WriteLine(helpStr);
                return 0;
            }

            BenchmarkConfig config = BenchmarkConfig.ParseSettingsFile(filename);
            if (config == null)
            {
                Console.WriteLine("Failed to parse benchmark config file");
                return 0;
            }

            BenchmarkInterface benchmarkInterface = new(config);
            benchmarkInterface.Start(config, oneshot);
        }
        else
        {
            Console.WriteLine(helpStr);
            return 0;
        }

        return 0;

    }



}

