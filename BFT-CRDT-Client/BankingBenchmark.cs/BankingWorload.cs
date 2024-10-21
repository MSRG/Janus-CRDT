using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BFTCRDT.Client;


public delegate void OnOpReceivedReply(bool result, string res);


public class BankingWorkload
{

    ConcurrentBag<(int, TimeSpan)> latansies = new();

    public uint sequenceNumber = 0;
    public ServerConnection conn;
    public ConcurrentDictionary<uint, OnOpReceivedReply> responseTracker = new();

    private readonly int[] sentCount = [0, 0, 0, 0];
    public BankingWorkload(ServerConnection conn)
    {
        this.conn = conn;
    }


    public void ViewBalance(string accountName)
    {
        uint seq = Interlocked.Increment(ref sequenceNumber);

        var msg = new ClientMessage() 
        { 
            sourceType = SourceType.Client,
            sequenceNumber = seq,
            key = accountName, 
            typeCode = "pnc", 
            opCode = "gp", 
            isSafe = false,
            paramsList = []
        };

        DateTime sendtime = DateTime.Now;
        sentCount[0]++;
        conn.Transmit(msg);

        AddToWaitResponse(seq, (bool result, string res) => 
        {
            if (!result || !int.TryParse(res, out _))
            {
                latansies.Add((0, TimeSpan.MaxValue));
            }
            else
            {
                latansies.Add((0, DateTime.Now - sendtime));
            }
        });
    }

    public void Deposit(string accountName, int amount)
    {
        uint seq = Interlocked.Increment(ref sequenceNumber);

        var msg = new ClientMessage() 
            { 
                sourceType = SourceType.Client,
                sequenceNumber = seq,
                key = accountName, 
                typeCode = "pnc", 
                opCode = "i", 
                isSafe = false,
                paramsList = [amount.ToString()] 
            };

        DateTime sendtime = DateTime.Now;
        sentCount[1]++;
        conn.Transmit(msg);
        
        AddToWaitResponse(seq, (bool result, string res) => 
        {
            if (!result)
            {
                latansies.Add((1, TimeSpan.MaxValue));
            }
            else
            {
                latansies.Add((1, DateTime.Now - sendtime));
            }
        });
    }

    public void Transfer(string accountName, string otherAccount, int amount)
    {
        uint seq = Interlocked.Increment(ref sequenceNumber);

        var msg = new ClientMessage() 
        { 
                sourceType = SourceType.Client,
                sequenceNumber = seq,
                key = accountName, 
                typeCode = "pnc", 
                opCode = "d", 
                isSafe = true,
                paramsList = [amount.ToString()] 
        };

        DateTime sendtime = DateTime.Now;
        sentCount[2]++;
        conn.Transmit(msg);

        AddToWaitResponse(seq, (bool result, string res) => 
        {
            if (!result)
            {
                latansies.Add((2, TimeSpan.MaxValue));
            }
            else
            {
                uint seq2 = Interlocked.Increment(ref sequenceNumber);

                var msg2 = new ClientMessage() 
                { 
                    sourceType = SourceType.Client,
                    sequenceNumber = seq2,
                    key = otherAccount, 
                    typeCode = "pnc", 
                    opCode = "i", 
                    isSafe = false,
                    paramsList = [amount.ToString()] 
                };
                conn.Transmit(msg2);

                // Console.WriteLine(msg2);
                // Console.WriteLine(responseTracker.TryGetValue(seq2, out _));

                AddToWaitResponse(seq2, (bool result, string res) => 
                {
                    if (!result)
                    {
                        latansies.Add((2, TimeSpan.MaxValue));
                    }
                    else
                    {
                        latansies.Add((2, DateTime.Now - sendtime));
                    }
                });


            }
        });
    }

    int dummyVariable = 0;
    public void Withdraw(string accountName, int amount)
    {
        uint seq = Interlocked.Increment(ref sequenceNumber);

        var msg = new ClientMessage() 
        { 
            sourceType = SourceType.Client,
            sequenceNumber = seq,
            key = accountName, 
            typeCode = "pnc", 
            opCode = "gs", 
            isSafe = false,
            paramsList = []
        };

        sentCount[3]++;

        DateTime sendtime = DateTime.Now;
        conn.Transmit(msg);

        AddToWaitResponse(seq, (bool result, string res) => {

            if (!result || !int.TryParse(res, out int balance))
            {
                latansies.Add((0, TimeSpan.MaxValue));
                return;
            }
            else
            {
                if (balance - amount < 0)
                {
                    // For the sake of the experiement, we do not check for invariant
                    // return; 
                    dummyVariable++; // stop compiler from optmizing this part
                }
            }

            uint seq2 = Interlocked.Increment(ref sequenceNumber);
            var msg2 = new ClientMessage() 
            { 
                    sourceType = SourceType.Client,
                    sequenceNumber = seq2,
                    key = accountName, 
                    typeCode = "pnc", 
                    opCode = "d", 
                    isSafe = true,
                    paramsList = [amount.ToString()] 
            };

            conn.Transmit(msg2);
            
            AddToWaitResponse(seq2, (bool result, string res) => 
            {
                if (!result)
                {
                    latansies.Add((3, TimeSpan.MaxValue));
                }
                else
                {
                    latansies.Add((3, DateTime.Now - sendtime));
                }
            });
        });

       
    }


    public void AddToWaitResponse(uint seq, OnOpReceivedReply afterRecivingReply)
    {
        if (!responseTracker.TryAdd(seq, afterRecivingReply))
        {
            throw new Exception("Seq number already exists");
        }
        
        
    }


    public int intervalRecvCount = 0;
    public void NotifyReceived(ClientMessage msg, Result resultTracker)
    {
        if (msg.sourceType == SourceType.Server)
        {
            if (responseTracker.TryRemove(msg.sequenceNumber, out OnOpReceivedReply onOpReceivedReply))
            {
                onOpReceivedReply(msg.result, msg.response);
                Interlocked.Increment(ref intervalRecvCount);
            }
        }
    }


    public void PopulateResults(in BankingBenchmarkResults results)
    {
        Interlocked.Add(ref results.totalView, sentCount[0]);
        Interlocked.Add(ref results.totalDeposit, sentCount[1]);
        Interlocked.Add(ref results.totalTransfer, sentCount[2]);
        Interlocked.Add(ref results.totalWithdraw, sentCount[3]);

        results.latencyResults.Add([.. latansies]);
        
        
        Console.Write(dummyVariable == 0);
    }
}

