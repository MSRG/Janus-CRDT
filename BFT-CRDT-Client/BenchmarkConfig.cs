using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BFTCRDT.Client;

public class BenchmarkConfig
{   
    [JsonInclude]
    public string[] addresses { get; set; }
    [JsonInclude]
    public int clientThreads { get; set; }
    [JsonInclude]
    public int duration { get; set; }
    [JsonInclude]
    public string typeCode { get; set; }
    [JsonInclude]
    public int numObjs { get; set; }
    [JsonInclude]
    public float[] opsRatio { get; set; }
    [JsonInclude]
    public float safeRatio { get; set; }
    [JsonInclude]
    public int targetOutputTPS { get; set; }

    public static BenchmarkConfig ParseSettingsFile(string filename)
    {
        BenchmarkConfig config = JsonSerializer.Deserialize<BenchmarkConfig>(File.ReadAllText(filename));
        if (!config.Verify())
            return null;
        
        return config;
    }

    public override string ToString()
    {
        StringBuilder sb = new();
        sb.Append("Benchmark Settings:").AppendLine();
        sb.Append("Addresses: ").Append(string.Join(", ", addresses)).AppendLine();
        sb.Append("Client Threads: ").Append(clientThreads).AppendLine();
        sb.Append("Duration: ").Append(duration).AppendLine();
        sb.Append("Type Code: ").Append(typeCode).AppendLine();
        sb.Append("Number of Objects: ").Append(numObjs).AppendLine();
        sb.Append("Operation Ratios: ").Append(string.Join(", ", opsRatio)).AppendLine();
        sb.Append("Safe Ratio: ").Append(safeRatio).AppendLine();
        sb.Append("Target Output TPS: ").Append(targetOutputTPS).AppendLine();

        return sb.ToString();
    }

    private bool Verify()
    {
        bool flag = true;
        if (clientThreads <= 0)
        {
            Console.WriteLine("Client threads must be greater than 0");
            flag = false;
        }
        if (duration <= 0)
        {
            Console.WriteLine("Duration must be greater than 0");
            flag = false;
        }
        if (numObjs <= 0)
        {
            Console.WriteLine("Number of objects must be greater than 0");
            flag = false;
        }
        if (Math.Abs(opsRatio.Sum() - 1) > 0.0001)
        {
            Console.WriteLine("Sum of operation ratios must be 1");
            flag = false;
        }
        if (safeRatio < 0 || safeRatio > 1)
        {
            Console.WriteLine("Safe ratio must be between 0 and 1");
            flag = false;
        }
        if (targetOutputTPS < 0)
        {
            Console.WriteLine("Target output TPS must be greater than 0 or 0 for off");
            flag = false;
        }


        return flag;
    }

}