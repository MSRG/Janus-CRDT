using System;
using System.Collections.Generic;
using MergeSharp;

namespace BFTCRDT;

public class ORSetWrapper : ISafeCRDTWrapper
{
    private ORSet<string> orset;

    public CRDT crdt 
    {
        get 
        {
            return orset;
        }
    }

    public ORSetWrapper(ORSet<string> orset)
    {
        this.orset = orset;
    }

    public object Query(object[] args = null)
    {
        string arg = (string)args[0];
        return orset.Contains(arg);
    }

    public object Update(int operationID, object[] args = null)
    {

        switch (operationID)
        {
            case 1:
                string arg = (string)args[0];
                return orset.Add(arg);
            case 2:
                arg = (string)args[0];
                return orset.Remove(arg);
            case 3:
                orset.Clear();
                return true;
            default:
                throw new InvalidOperationException("Invalid ORSet method name"); 
        }
    }
}