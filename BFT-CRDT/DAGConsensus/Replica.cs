using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace BFTCRDT.DAG;

public class Replica
{
    public int nodeid { get; }

    public ECDsa privateKey { get; } = null;
    private byte[] _publicKey;
    public byte[] publicKey
    {
        get => this._publicKey;
        set
        {
            this._publicKey = value;
            publicKeyHandler = ECDsa.Create();
            publicKeyHandler.ImportSubjectPublicKeyInfo(this._publicKey, out _);
        }

    }

    // ECDSA instances for the public key for verifying signatures
    public ECDsa publicKeyHandler { get; private set; }


    /// <summary>
    /// Creating a replica.
    /// </summary>
    /// <param name="nodeid"></param>
    /// <param name="privatekey">Whether to create a new public-private key-pair</param>
    public Replica(int nodeid, bool createKey = true)
    {
        this.nodeid = nodeid;
        if (createKey)
        {
            this.privateKey = ECDsa.Create();
            this.publicKey = this.privateKey.ExportSubjectPublicKeyInfo();
        }
    }

    public static List<Replica> GenerateReplicas(List<int> nodeids, int selfNodeid, out Replica self)
    {
        List<Replica> replicas = new();
        foreach (var n in nodeids)
        {
            if (n == selfNodeid)
                continue;
            else
            {
                replicas.Add(new(n, false));
            }
        }

        self = new(selfNodeid);
        replicas.Add(self);

        // sort by id
        replicas.Sort((x, y) => x.nodeid.CompareTo(y.nodeid));

        return replicas;

    }


}