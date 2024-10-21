using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ProtoBuf;

namespace BFTCRDT.Client;

public delegate void HandleReceived(ClientMessage msg, Result resultTracker);

public class ServerConnection
{
    public string ip { get; private set; }
    public int port { get; private set; } 

    CancellationTokenSource cts = new CancellationTokenSource();

    private TcpClient tcpClient;

    private volatile bool isReceiving = false;

    public ServerConnection(string ip, int port)
    {
        this.ip = ip;
        this.port = port;
    }

    public bool Connect()
    {
        try
        {
            this.tcpClient = new TcpClient(ip, port);
            tcpClient.SendTimeout = 10000;
        }
        catch (SocketException)
        {
            Console.WriteLine($"Failed to connect to server at {ip}:{port}");
            return false;
        }

        return true;
    }

    public void Transmit(ClientMessage message)
    {        
        try
        {
            NetworkStream ns = tcpClient.GetStream();
            Serializer.SerializeWithLengthPrefix(ns, message, PrefixStyle.Base128);
        }
        catch (IOException e)
        {
            if (e.Message.Contains("timed out"))
            {
                Console.WriteLine($"{DateTime.Now} Msg to {ip}:{port} timed out");
                return;
            }
            else
            {
                throw;
            }
        }
    }

    public void StartReceiveResult(HandleReceived handleReceived, Result resultTracker)
    {
        isReceiving = true;

        var ct = cts.Token;


        Task.Run(() =>
        {
            NetworkStream ns = tcpClient.GetStream();
            while (isReceiving)
            {
                try
                {
                    ClientMessage msg = Serializer.DeserializeWithLengthPrefix<ClientMessage>(ns, PrefixStyle.Base128);
                    //Console.WriteLine($"Received {res} at {DateTime.Now}");
                    if (msg is not null)
                    {
                        handleReceived(msg, resultTracker);
                    }
                }
                catch (IOException)
                {
                    //Console.WriteLine($"Connection to server at {ip}:{port} lost");
                    return;
                }
            }
        }, ct);



    } 

    public void StopReceiveResult()
    {
        isReceiving = false;
        cts.Cancel();
    }
    

    public void Close()
    {
        isReceiving = false;
        tcpClient.Close();
    }
}