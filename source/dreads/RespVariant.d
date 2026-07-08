module dreads.RespVariant;
import std.json;

import std.algorithm : map, joiner;
import std.array : array, join, replace;
import std.conv : to, text;
import std.ascii : isPrintable;
import std.string : capitalize, format;
import std.range : repeat, enumerate;
import std.bigint : BigInt;

public enum Color
{
    red = "\033[31m",
    green = "\033[32m",
    blue = "\033[34m",
    yellow = "\x1b[33m",
    magenta = "\x1b[35m",
    cyan = "\x1b[36m",
    white = "\x1b[37m",
    black = "\x1b[30m",

    brightRed = "\x1b[91m",
    brightGreen = "\x1b[92m",
    brightYellow = "\x1b[93m",
    brightBlue = "\x1b[94m",
    brightMagenta = "\x1b[95m",
    brightCyan = "\x1b[96m",
    brightWhite = "\x1b[97m",
    brightBlack = "\x1b[90m",
    reset = "\033[0m"
}

mixin template generateColorFunctions(T)
{
    static foreach (m; __traits(allMembers, T))
    {
        mixin(q{public string to} ~ m[0].to!string.capitalize ~ m[1 .. $] ~ "(string value){
            return Color."
                ~ m ~ " ~ value ~ Color.reset;}"
        );
    }
}

mixin generateColorFunctions!Color;

public string escapeJson(string input)
{
    return input
        .replace(`\`, `\\`)
        .replace(`"`, `\"`);
}

public string escapeBinary(string input)
{
    string result;
    foreach (b; input)
    {
        switch (b)
        {
        case '\n':
            result ~= `\n`;
            break;
        case '\t':
            result ~= `\t`;
            break;
        case '\\':
            result ~= "\\\\";
            break;
        default:
            if (b.isPrintable)
                result ~= cast(char) b;
            else
                result ~= format("\\x%02X", b);

        }

    }
    return result;
}

public enum RespVariantType
{
    String, //
    BString, //
    Int, //
    Array, //
    Map,
    Boolean,
    Null,
    BigInt, //
    Double,
    Set,
    Push,
    Error, //
    BError, //
    Attribute,
    Verbatim

}

struct Dummy_
{
}

public class RVariant
{
    RespVariantType type;
    bool error;
    union
    {
        string s;
        int i;
        double d;
        bool b;
        BigInt bi;
        RVariant[] arr;
        RVariant[string] m;
    }

    static RVariant fromBulk(string s)
    {
        auto r = new RVariant(Dummy_());
        r.s = s;
        r.type = RespVariantType.BString;
        return r;
    }

    static RVariant newArray()
    {
        auto r = new RVariant(Dummy_());
        r.type = RespVariantType.Array;
        r.arr = [];
        return r;
    }

    @disable this();

    this(Dummy_)
    {
    }

    this(string s, bool error = false)
    {
        type = error ? RespVariantType.Error : RespVariantType.String;
        this.error = error;
        this.s = s;
    }

    this(int i)
    {
        type = RespVariantType.Int;
        this.i = i;
    }

    this(BigInt bi)
    {
        type = RespVariantType.BigInt;
        this.bi = bi;
    }

    this(double d)
    {
        type = RespVariantType.Double;
        this.d = d;
    }

    this(bool b)
    {
        type = RespVariantType.Boolean;
        this.b = b;
    }

    this(RVariant[] a)
    {
        type = RespVariantType.Array;
        arr = a;
    }

    this(RVariant[string] m, RespVariantType type = RespVariantType.Map)
    in
    {
        assert(type == RespVariantType.Map || type == RespVariantType.Attribute);
    }
    body
    {
        this.type = type;
        this.m = m;
    }

    override string toString() const
    {
        return "RVariant(" ~ _toString ~ ")";

    }

    JSONValue toJson() const
    {
        switch (type)
        {
        case RespVariantType.Int:
            return JSONValue(i);
        case RespVariantType.Error:
            return JSONValue("Error: " ~ s);
        case RespVariantType.String:
            return JSONValue(s);
        case RespVariantType.Array:
            {
                JSONValue json;
                json.array = arr.map!(x => x.toJson()).array;
                return json;
            }
        default:
            assert(0);

        }
    }

    private string _toString(ulong indent = 0) const
    {
        string indentStr = " ".repeat(indent * 2).joiner.text;
        string nextIndentStr = " ".repeat((indent + 1) * 2).joiner.text;

        switch (type)
        {
        case RespVariantType.Error:
            return indentStr ~ "\"" ~ s.escapeBinary.toRed ~ "\"";
        case RespVariantType.Int:
            return indentStr ~ i.to!string.toBlue;
        case RespVariantType.String:
            return indentStr ~ "\"" ~ s.escapeBinary.toGreen ~ "\"";
        case RespVariantType.BString:
            return indentStr ~ "*\"" ~ s.escapeBinary.toBrightCyan ~ "\"";
        case RespVariantType.BError:
            return indentStr ~ "*\"" ~ s.escapeBinary.toRed ~ "\"";
        case RespVariantType.Array:
            {
                string[] output = [indentStr ~ "["];
                foreach (i, key; arr)
                {
                    bool isLast = (i == arr.length - 1);
                    auto o = key._toString(indent + 1);
                    if (!isLast)
                        o ~= ",";
                    output ~= o;
                }
                output ~= indentStr ~ "]";
                return output.join("\n");
            }
        case RespVariantType.Boolean:
            return b.to!string.toBrightBlue;
        case RespVariantType.Double:
            return d.to!string.toBlue;
        case RespVariantType.Push:

            {
                string[] output = [indentStr ~ ">["];
                foreach (i, key; arr)
                {
                    bool isLast = (i == arr.length - 1);
                    auto o = key._toString(indent + 1);
                    if (!isLast)
                        o ~= ",";
                    output ~= o;
                }
                output ~= indentStr ~ "]";
                return output.join("\n");
            }

        default:
            assert(0);
        }
    }
}
