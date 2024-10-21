using System;
using ProtoBuf;

namespace BFTCRDT;

public enum SourceType : byte
{
    None,
    Client,
    Server
}

[ProtoContract]
public class ClientMessage
{
    [ProtoMember(1)]
    public SourceType sourceType;
    [ProtoMember(2)]
    public uint sequenceNumber;
    [ProtoMember(3)]
    public string key;
    [ProtoMember(4)]
    public string typeCode;
    [ProtoMember(5)]
    public string opCode;
    [ProtoMember(6)]
    public bool isSafe;
    [ProtoMember(7)]
    public string[] paramsList;
    [ProtoMember(8)]
    public bool result = false;
    [ProtoMember(9)]
    public string response = "";


    public override string ToString()
    {
        // check every field for null
        string sourceType = this.sourceType.ToString();
        string sequenceNumber = this.sequenceNumber.ToString();
        string key = this.key != null ? this.key : "null";
        string typeCode = this.typeCode != null ? this.typeCode : "null";
        string opCode = this.opCode != null ? this.opCode : "null";
        string isSafe = this.isSafe.ToString();
        string result = this.result.ToString();
        string response = this.response != null ? this.response : "null";
        string paramsListStr = paramsList != null ? string.Join(",", paramsList) : "null";
        string responseStr = response != null ? response : "null";
        return $"[ClientMessage: sourceType={sourceType}, seq={sequenceNumber} key={key}, typeCode={typeCode}, opCode={opCode}, isSafe={isSafe}, paramsList={paramsListStr}, result={result}, response={responseStr}]";
    }

}