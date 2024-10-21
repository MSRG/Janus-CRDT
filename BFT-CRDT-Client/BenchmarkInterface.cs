using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ProtoBuf;

namespace BFTCRDT.Client;

public class BenchmarkInterface
{
    private BenchmarkConfig config;

    public BenchmarkInterface(BenchmarkConfig config)
    {
        this.config = config;
    }

    /// <summary>
    /// that measures and print throughput and average latency of a given period of time.
    /// </summary>
    /// <param name="duration"></param>
    public void Start(BenchmarkConfig benchmarkConfig, bool oneshot = false)
    {
        Serializer.PrepareSerializer<ClientMessage>();

        while (true)
        {
            if (!oneshot)
            {
                Console.WriteLine("Type s to start benchmark and exit to cancel");
                while (true)
                {
                    string input = Console.ReadLine();
                    if (input == "s")
                    {
                        break;
                    }
                    else if (input == "exit")
                    {
                        return;
                    }
                    else
                    {
                        Console.WriteLine("Invalid command");
                    }
                }
            }

            Console.WriteLine("Initializing benchmark with the following settings:");
            Console.WriteLine(benchmarkConfig.ToString());

            try 
            {
                if (benchmarkConfig.typeCode.Equals("bank"))
                {
                    benchmarkConfig.typeCode = "pnc";
                    BankingBenchmarkRunner bankRunner = new(benchmarkConfig);
                    bankRunner.Init();
                    BankingBenchmarkResults results = bankRunner.Run();
                    Console.WriteLine("\nBenchmark Completed");
                    Thread.Sleep(1000);
                    results.Parse();
                }
                else
                {
                    BenchmarkRunners runner = new(benchmarkConfig);
                    runner.Init();

                    List<Result> results = runner.Run(oneshot);
                    Console.WriteLine("\nBenchmark Completed");
                    Thread.Sleep(1000);
                    Result.PrintResult(results, benchmarkConfig);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Benchmark failed because:");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }

            if (oneshot)
                return;

        }

    }
}




