using System;

namespace BFTCRDT.DAG;

public class Stats : ICloneable
{
    volatile public int certificateByTimeout = 0;
    volatile public int certificateByPreemptiveCheck = 0;
    volatile public int receivedUpdates = 0;
    //public List<int> numUpdatesPerbklock = new();
    volatile public int numConsensusCompleted = 0;
    volatile public int numUpdatesfinishedConsensus = 0;
    volatile public int missingPredecessorCount = 0;
    volatile public int noCertificateCheckCount = 0;
    volatile public int noAdvanceRoundBecauseNotThereYetCount = 0;
    volatile public int jumpingRoundCount = 0;
    volatile public int callingAdvanceRoundCount = 0;
    volatile public int actuallyCreatedBlockCount = 0;
    volatile public int checkCertificateLockTimeoutCount = 0;
    volatile public int rejectByInvalidSignatureCount = 0;

    volatile public int receivedBlockMsgCount = 0;
    volatile public int receivedCertMsgCount = 0;
    volatile public int receivedAckMsgCount = 0;
    volatile public int receivedBlockQueryMsgCount = 0;
    volatile public int sentBlockMsgCount = 0;
    volatile public int sentCertMsgCount = 0;
    volatile public int sentAckMsgCount = 0;
    volatile public int sentBlockQueryMsgCount = 0;
    volatile public int notEnoughCertToAdvanceCount = 0;

    public Stats() 
    {
    }

    public string[] GetCurrStats()
    {
        return new string[] {   
                                $"{receivedBlockMsgCount}",
                                $"{receivedCertMsgCount}",
                                $"{receivedAckMsgCount}",
                                $"{receivedBlockQueryMsgCount}",
                                "-",
                                $"{sentBlockMsgCount}",
                                $"{sentCertMsgCount}",
                                $"{sentAckMsgCount}",
                                $"{sentBlockQueryMsgCount}",
                                "|",
                                $"{certificateByTimeout}",
                                $"{certificateByPreemptiveCheck}",
                                $"{receivedUpdates}",
                                $"{noCertificateCheckCount}",
                                $"{missingPredecessorCount}",
                                "|",
                                $"{noAdvanceRoundBecauseNotThereYetCount}",
                                $"{jumpingRoundCount}",
                                $"{callingAdvanceRoundCount}",
                                $"{checkCertificateLockTimeoutCount}",
                                $"{notEnoughCertToAdvanceCount}"};

    }

    public object Clone()
    {
        return MemberwiseClone();
    }
}