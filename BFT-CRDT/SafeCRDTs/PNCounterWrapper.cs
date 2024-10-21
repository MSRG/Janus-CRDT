using System;
using System.Collections.Generic;
using MergeSharp;

namespace BFTCRDT;


public class PNCounterWrapper : ISafeCRDTWrapper
{

    private PNCounter pnc;

    public CRDT crdt 
    {
        get 
        {
            return pnc;
        }
    }

    public PNCounterWrapper(PNCounter pnc)
    {
        this.pnc = pnc;
    }



    public object Query(object[] args = null)
    {
        return pnc.Get();
    }

    public object Update(int operationID, object[] args = null)
    {
        int arg = (int)args[0];

        switch (operationID)
        {
            case 1:
                pnc.Increment(arg);
                return true;
            case 2:
                pnc.Decrement(arg);
                return true;
            default:
                throw new InvalidOperationException("Invalid PNC method name"); 
        }
    }
}
