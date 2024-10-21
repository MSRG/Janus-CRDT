using Xunit;
using BFTCRDT;
using PBFTConnectionManager;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace Tests;

public class OpHistoryTests
{

    [Fact]
    public void TestOpHistory()
    {
        OpHistoryLog ophistory1 = new OpHistoryLog(1, RCRDTCode.RCounter);
        OpHistoryLog ophistory2 = new OpHistoryLog(2, RCRDTCode.RCounter);

        ReversibleCounter counter1 = new ReversibleCounter();
        ReversibleCounter counter2 = new ReversibleCounter();

        counter1.Increment(5);  
        Update u1 = new Update(1) { delta = counter1.LastLastUpdateDelta };
        ophistory1.AddUpdate(ref u1);

        var propmsg = counter1.GetLastSynchronizedUpdate();
        var u1updatemsg = new OpHistoryUpdateMessage("", u1, ophistory1.rcrdtCode);
        
        counter2.ApplySynchronizedUpdate(propmsg);
        ophistory2.InsertSyncedUpdate(u1updatemsg.ConvertToUpdate());



        Assert.Equal(ophistory1.tails[0].hashId, ophistory2.tails[0].hashId);
        Assert.Equal(ophistory1.tails[0].senderId, ophistory2.tails[0].senderId);
        // compare prev hash as a byte array use ByteArrayCompare class
        Assert.True(ophistory1.tails[0].prevHash[0].SequenceEqual(ophistory2.tails[0].prevHash[0]));
        Assert.Equal(counter1.Get(), counter2.Get());
    }

    [Fact]
    public void TestOpHistoryConcurrency()
    {
        OpHistoryLog ophistory1 = new OpHistoryLog(1, RCRDTCode.RCounter);
        OpHistoryLog ophistory2 = new OpHistoryLog(2, RCRDTCode.RCounter);

        ReversibleCounter counter1 = new ReversibleCounter();
        ReversibleCounter counter2 = new ReversibleCounter();

        counter1.Increment(5);  
        Update u1 = new Update(1) { delta = counter1.LastLastUpdateDelta };
        ophistory1.AddUpdate(ref u1);
        var propmsgu1 = counter1.GetLastSynchronizedUpdate();
        var u1updatemsg = new OpHistoryUpdateMessage("", u1, ophistory1.rcrdtCode);

        counter2.Increment(7);  
        Update u2 = new Update(2) { delta = counter1.LastLastUpdateDelta };
        ophistory2.AddUpdate(ref u2);
        var propmsgu2 = counter2.GetLastSynchronizedUpdate();
        var u2updatemsg = new OpHistoryUpdateMessage("", u2, ophistory2.rcrdtCode);

        counter1.ApplySynchronizedUpdate(propmsgu2);
        ophistory1.InsertSyncedUpdate(u2updatemsg.ConvertToUpdate());

        counter2.ApplySynchronizedUpdate(propmsgu1);
        ophistory2.InsertSyncedUpdate(u1updatemsg.ConvertToUpdate());

        Assert.Equal(counter1.Get(), counter2.Get());
        Assert.True(ophistory1.tails[0].hashId.SequenceEqual(ophistory2.tails[1].hashId));
        Assert.True(ophistory1.tails[1].hashId.SequenceEqual(ophistory2.tails[0].hashId));

        counter1.Increment(3);
        Update u3 = new Update(1) { delta = counter1.LastLastUpdateDelta };
        ophistory1.AddUpdate(ref u3);
        var propmsgu3 = counter1.GetLastSynchronizedUpdate();
        var u3updatemsg = new OpHistoryUpdateMessage("", u3, ophistory1.rcrdtCode);

        counter2.ApplySynchronizedUpdate(propmsgu3);
        ophistory2.InsertSyncedUpdate(u3updatemsg.ConvertToUpdate());

        Assert.Equal(counter1.Get(), counter2.Get());
        Assert.True(ophistory1.tails[0].hashId.SequenceEqual(ophistory2.tails[0].hashId));
    }

    [Fact]
    public void TestOphistoryVerify()
    {
        Assert.True(false); // TODO: 
    }


}