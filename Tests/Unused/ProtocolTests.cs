using Xunit;
using PBFTConnectionManager;
using ProtoBuf;
using System.IO;
using System;

namespace Tests;

public class ProtocolTests  : IDisposable
{

    public ProtoBuf.TypeResolver resolver;
    
    // setups
    public ProtocolTests()
    {
        // setup
        resolver = MessageTypes.ResolveType;
    }

        // teardown
    public void Dispose()
    {
        // if file still exists, delete the file
        if (File.Exists("test.bin"))
        {
            File.Delete("test.bin");
        }

    }


    [Fact]
    public void TestSingleMessage()
    {

        HeartBeat testMsg = new();
        testMsg.sender = 1;


        using (var file = File.Create("test.bin"))
        {
            Serializer.Serialize(file, testMsg);
        }

        HeartBeat deserialized;
        using (var file = File.OpenRead("test.bin"))
        {
            deserialized = Serializer.Deserialize<HeartBeat>(file);
        }

        // assert
        Assert.Equal(testMsg.sender, deserialized.sender);
        
        // delete file
        File.Delete("test.bin");
    }

    [Fact]
    public void TestSingleMessage2()
    {

        HeartBeat testMsg = new();
        testMsg.sender = 1;


        MemoryStream ms = new();
        Serializer.Serialize(ms, testMsg);
        
        ms.Position = 0;
        HeartBeat deserialized;

        deserialized = Serializer.Deserialize<HeartBeat>(ms);
        

        // assert
        Assert.Equal(testMsg.sender, deserialized.sender);
        
        // delete file
        File.Delete("test.bin");
    }


    [Fact]
    public void TestSingleMessageWithHeader()
    {
        HeartBeat testMsg = new();
        testMsg.sender = 1;

        using (var file = File.Create("test.bin"))
        {
            Serializer.SerializeWithLengthPrefix<HeartBeat>(file, testMsg, PrefixStyle.Base128, 2);
        }

        object deserialized;
        using (var file = File.OpenRead("test.bin"))
        {
            Serializer.NonGeneric.TryDeserializeWithLengthPrefix(file, PrefixStyle.Base128, resolver, out deserialized);
        }    

        // assert
        Assert.Equal(testMsg.sender, ((HeartBeat)deserialized).sender);
    }

    [Fact]
    public void TestManyMessagesWithHeader()
    {
        HeartBeat testHeartBeat = new();
        testHeartBeat.sender = 1;

        PBFTMessage testPBFTMessage = new(PBFTMessageType.None, 0);
        testPBFTMessage.message = "test1";

        PBFTMessage testPBFTMessage2 = new(PBFTMessageType.None, 1);
        testPBFTMessage2.message = "test2";

        using (var file = File.Create("test.bin"))
        {
            Serializer.SerializeWithLengthPrefix<HeartBeat>(file, testHeartBeat, PrefixStyle.Base128, 2);
            Serializer.SerializeWithLengthPrefix<PBFTMessage>(file, testPBFTMessage, PrefixStyle.Base128, 3);
            Serializer.SerializeWithLengthPrefix<PBFTMessage>(file, testPBFTMessage2, PrefixStyle.Base128, 3);
        }

        object deserialized;
        using (var file = File.OpenRead("test.bin"))
        {
            while (Serializer.NonGeneric.TryDeserializeWithLengthPrefix(file, PrefixStyle.Base128, resolver, out deserialized))
            {
                if (deserialized is HeartBeat)
                {
                    Assert.Equal(testHeartBeat.sender, ((HeartBeat)deserialized).sender);
                }
                else if (deserialized is PBFTMessage)
                {
                    if (((PBFTMessage)deserialized).sequence == 1)
                    {
                        Assert.Equal(testPBFTMessage.message, ((PBFTMessage)deserialized).message);
                    }
                    else if (((PBFTMessage)deserialized).sequence == 2)
                    {
                        Assert.Equal(testPBFTMessage2.message, ((PBFTMessage)deserialized).message);
                    }
                }
            }
        }



    }


}