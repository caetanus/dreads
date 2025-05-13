module dedis.resp;
import std.stdio;
import std.string : indexOf;
import std.algorithm;
import std.ascii : isPrintable;
import std.format;
import std.traits;
import core.exception;

import std.range;
import std.conv;
import std.exception;
import std.meta : AliasSeq;
import std.string : capitalize;
import std.json;

import dreads.RespVariant;

public enum RespType
{
    String = '+',
    BString = '$',
    Verbadim = '|',
    Error = '-',
    BError =  '!',
    Integer = ':',
    BigNumber = '(',
    Double = ',',
    Boolean = '#',
    Null = '_',
    Array = '*',
    Map = '%',
    Set = '~',
    Push = '>',
    Attribute = '='

}

public class RespError : Exception
{
    this(string msg)
    {
        super(msg);
    }

}

void debugPrint(T...)(T args)
{

    //debug writeln(args);

}

int indexOf(ubyte[] haystack, ubyte[] needle)
{
    if (needle.length == 0)
    {
        return 0;
    }
    outer: foreach (i; 0 .. haystack.length - needle.length + 1)
    {
        foreach (j; 0 .. needle.length)
        {
            if (haystack[i + j] != needle[j])
            {
                continue outer;
            }
        }
        return i.to!int;

    }
    return -1;
}

public struct MemoryStream
{
    ubyte[] buffer;
    size_t cursor;

    this(ubyte[] data)
    {
        buffer = data.dup; // faz uma cópia defensiva
        cursor = 0;
    }

    /// Lê até `n` bytes
    ubyte[] read(size_t n)
    {
        enforce(cursor + n <= buffer.length, "Read além do fim!");
        auto slice = buffer[cursor .. cursor + n];
        cursor += n;
        return slice;
    }

    /// Lê até encontrar um delimitador (ex: "\r\n")
    ubyte[] readUntil(string delim)
    {
        auto idx = buffer[cursor .. $].indexOf(cast(ubyte[]) delim);
        enforce(idx != -1, "Delimitador não encontrado!");
        auto slice = buffer[cursor .. cursor + idx];
        cursor += idx + delim.length;
        return slice;
    }

    void[] readLine()
    {
        return cast(void[]) readUntil("\n");
    }

    /// Escreve bytes no final do buffer
    void write(ubyte[] data)
    {
        buffer ~= data;
    }

    /// Volta para o início
    void rewind()
    {
        cursor = 0;
    }

    bool eof()
    {
        return cursor >= buffer.length;
    }

    string data() const
    {
        return cast(string)(buffer.dup());
    }

    string toString() const
    {
        return data;
    }
}

/+unittest
{
    RVariant[] v = [
        new RVariant(1), new RVariant("test\x01\x02\x03"), new RVariant("error", true)
    ];
    RVariant[] v2 = [
        new RVariant(2), new RVariant("t\"es\\t2"), new RVariant("-1\x01a", true), new RVariant(v)
    ];

    RVariant[] v1 = [
        new RVariant(v.dup), new RVariant(v.dup), new RVariant(v2), new RVariant(30),
        new RVariant("error", true), new RVariant("oi")
    ];

    auto o = new RVariant(v1);
    assert(o.toJson.to!string == q{[[1,"test\u0001\u0002\u0003","Error: error"],[1,"test\u0001\u0002\u0003","Error: error"],[2,"t\"es\\t2","Error: -1\u0001a",[1,"test\u0001\u0002\u0003","Error: error"]],30,"Error: error","oi"]});

}

unittest
{
    import dreads.logo;
    writeln(logo);

}+/

public RVariant RespParser(T)(ref T input)
        if (__traits(compiles, input.readLine) &&
        __traits(compiles, input.read))
{
    int getNumber(ref MemoryStream input)
    {
        try
        {
            auto data = (cast(string) input.readLine())[0 .. $ - 1].to!int;
            debugPrint("getNumber, ", data);
            return data;
        }
        catch (ConvException e)
        {
            throw new RespError("invalid number");
        }
        catch (UnicodeException)
        {
            throw new RespError("invalid number");
        }

    }

    char type = input.read(1)[0];
    switch (type)
    {
    case RespType.String:
        {
            auto data = (cast(string) input.readLine)[0 .. $ - 1];
            debugPrint("read string: ", data);
            return new RVariant(data);
        }
    case RespType.Error:
        {
            auto data = (cast(string) input.readLine)[0 .. $ - 1];
            debugPrint("read error string: ", data);
            return new RVariant(data, true);
        }
    case RespType.Integer:
        {
            auto data = getNumber(input);
            debugPrint("read number: ", data);
            return new RVariant(data);
        }
    case RespType.BulkString:
        try
        {
            int number = getNumber(input);
            debugPrint("bulk string, size: ", number);
            if (number < 0)
            {
                return new RVariant("");
            }
            auto output = cast(string) input.read(number);
            debugPrint("bstring: ", output);
            input.readLine();
            return RVariant.fromBulk(output);
        }
        catch (ConvException e)
        {
            throw new RespError("invalid number");
        }
        catch (UnicodeException)
        {
            throw new RespError("invalid number");
        }
    case RespType.Array:
        int items = getNumber(input);
        debugPrint("array, size: ", items);
        if (items < 0)
        {
            return RVariant.newArray;
        }
        RVariant[] output;

        for (int i = 0; i < items; i++)
        {
            output ~= RespParser(input);
        }
        return new RVariant(output);
    default:
        assert(0);

    }
}
/+
unittest
{
    string insaneResp = "*1\r\n*1\r\n*1\r\n*1\r\n*5\r\n$6\r\nhello\n\r\r\n$-1\r\n:-12345\r\n+OK\r\n-Something went wrong\r\n*3\r\n$5\r\ninner\r\n$4\r\ntest\r\n$8\r\n\0\1A\nB\r\xFF\r\n";

    string[] complexResp = [
        "*8\r\n+OK\r\n-ERR something went wrong\r\n:42\r\n$6\r\nfoo\x02ar\r\n$-1\r\n$0\r\n\r\n" ~ "*2\r\n+foo\r\n+bar\r\n*0\r\n",
        "*7\r\n$6\r\nsimple\r\n:42\r\n:-999\r\n$-1\r\n$13\r\nhello\nworld\x00\n\r\n$14\r\n\0\1ABC\n\r\xFF\r\n*3\r\n+OK\r\n-ERR something went wrong\r\n$0\r\n\r\n",
        "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        insaneResp    ];

    foreach (input; complexResp)
    {
        writeln("testing: ", input.escapeBinary);
        auto ms = MemoryStream(cast(ubyte[]) input);
        //auto output = RespParser(ms);
        //writeln(output);
    }
}
+/

//string[string[Variant]] RespParser(MemoryStream input) {
//    string readString (string data) {
//        auto idx = data.indexOf("\r\n".to!(ubyte[]));
//        return idx[0 .. idx];
//    }
//    int readInteger (string data) {
//        auto idx = data.indexOf("\r\n".to!(ubyte[]));
//        return idx[0 .. idx].to!int;
//    }
//    //string[Variant] readArray(MemoryStream input) {
//    //    auto data = input.readUntil("\r\n");
//    //    try {
//    //        auto idata = data.to!int;
//    //        for(int i = 0; i < idata; i++) {
//    //            switch (input.read(1)) {
//    //                case RespType.Array:
//    //                auto out = readArray(input);
//    //            }
//    //        }
//    //    }
//
//    //}
//    auto line = input.readUntil("\r\n");
//    return [];
//
//
//}
