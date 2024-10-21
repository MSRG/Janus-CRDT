using System;
using System.Collections.Generic;
using System.Linq;


namespace BFTCRDT;

public class ByteArrayComparer : IEqualityComparer<byte[]> 
{
    public bool Equals(byte[] a, byte[] b)
     {
        return a.SequenceEqual(b);
    }

    public int GetHashCode(byte[] key) 
    {
        // get the first 4 bytes and convert it to an int
        return BitConverter.ToInt32(key, 0);
    }
}