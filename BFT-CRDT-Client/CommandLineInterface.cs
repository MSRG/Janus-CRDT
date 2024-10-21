using System;
using Microsoft.Extensions.Logging;
using BFTCRDT;

namespace BFTCRDT.Client;



public class CommandLineInterface
{
    private ServerConnection serverConnection;

    public CommandLineInterface(ServerConnection serverConnection)
    {
        this.serverConnection = serverConnection;
    }

    public void Start()
    {
        Console.WriteLine("Command Format: [type] [key] [op] [isSafe] [params]\n Type exit to stop");
        
        serverConnection.StartReceiveResult(PrintResult, new Result());

        uint sequenceNumber = 1;

        while (true)
        {
            string input = Console.ReadLine();
            if (input == "exit")
            {
                break;
            }
            else
            {
                Console.WriteLine("Handling Request:\n{0}", input);

                string response = "";
                try
                {

                    try 
                    {
                        var msg = CommandParser.Parse(input);
                        msg.sequenceNumber = sequenceNumber;
                        serverConnection.Transmit(msg);
                    }
                    catch (ArgumentException)
                    {
                        Console.WriteLine("Invalid command format");
                    }

                    
                } 
                catch (Exception e)
                {
                    Console.WriteLine($"Command failed, because of {e.Message} at {e.StackTrace}");
                }
                 
                sequenceNumber++;
            }
        }
    }

    
    public void PrintResult(ClientMessage msg, Result resultTracker)
    {

        Console.WriteLine($"Received result for command {msg.sequenceNumber}: it is {msg.result} with response {msg.response} ");

    }

}


