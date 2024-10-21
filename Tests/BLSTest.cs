using Xunit;
using System;
using Planetarium.Cryptography.BLS12_381;
using System.Text;

namespace Tests;

/// IMPORTANT:
/// Before the BLS library can be used, have to make sure /usr/lib has the "libdl.so" file 
/// Copy one from other places if necessary, for example, copy and rename /usr/lib/x86_64-linux-gnu/libdl.so.2 

[Collection("collection1")]
public class BLSLibTests : IDisposable
{
    public BLSLibTests()
    {

    }

    public void Dispose()
    {
       
    }

    // [Fact]
    // public void TestSimple()
    // {
    //     SecretKey sec;
    //     sec.SetByCSPRNG(); // init secret key
    //     PublicKey pub = sec.GetPublicKey(); // get public key
    //     Msg msg = new Msg();
    //     string m = "abc";

    //     msg.Set(Encoding.Convert(Encoding.UTF8, Encoding.ASCII, Encoding.UTF8.GetBytes(m)));
    //     Signature sig = sec.Sign(msg); // create signature
    //     Assert.True(pub.Verify(sig, msg));

        
    // }



}