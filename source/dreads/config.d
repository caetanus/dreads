module dreads.config;

// redis.conf-style configuration: one 'directive value...' per line, '#'
// comments. The format is trivial by design — hand parser per project rule
// (pegged only if the grammar ever outgrows lines).

import std.string : strip;

public struct Config
{
    ushort port = 6379;
    bool appendonly = false;
    string appendfilename = "dreads.aof";
    string dir; // working directory (chdir at boot)
    ulong maxmemory = 0; // bytes; 0 = unlimited
    string maxmemoryPolicy = "noeviction"; // noeviction | allkeys-lru | volatile-lru
}

/// The live configuration (CONFIG GET/SET read and mutate it).
public __gshared Config gConfig;

/// "100mb"-style sizes. Returns false on garbage.
public bool parseMemory(string s, out ulong bytes) nothrow
{
    import std.conv : to;

    if (s.length == 0)
        return false;
    ulong mult = 1;
    auto num = s;
    void suffix(size_t n, ulong m)
    {
        num = s[0 .. s.length - n];
        mult = m;
    }

    if (s.length > 2)
    {
        auto tail = s[$ - 2 .. $];
        if (tail == "kb" || tail == "KB" || tail == "Kb")
            suffix(2, 1024);
        else if (tail == "mb" || tail == "MB" || tail == "Mb")
            suffix(2, 1024UL * 1024);
        else if (tail == "gb" || tail == "GB" || tail == "Gb")
            suffix(2, 1024UL * 1024 * 1024);
    }
    if (mult == 1 && s.length > 1 && (s[$ - 1] == 'k' || s[$ - 1] == 'K'))
        suffix(1, 1000);
    else if (mult == 1 && s.length > 1 && (s[$ - 1] == 'm' || s[$ - 1] == 'M'))
        suffix(1, 1_000_000);
    else if (mult == 1 && s.length > 1 && (s[$ - 1] == 'g' || s[$ - 1] == 'G'))
        suffix(1, 1_000_000_000);
    try
        bytes = num.to!ulong * mult;
    catch (Exception)
        return false;
    return true;
}

private string unquote(string s) nothrow
{
    if (s.length >= 2 && ((s[0] == '"' && s[$ - 1] == '"') || (s[0] == '\'' && s[$ - 1] == '\'')))
        return s[1 .. $ - 1];
    return s;
}

/// Applies one directive; false = unknown or invalid value.
public bool applyDirective(string name, string value, ref Config cfg) nothrow
{
    import std.conv : to;
    import std.uni : toLower;

    string lname;
    try
        lname = name.toLower;
    catch (Exception)
        return false;
    switch (lname)
    {
    case "port":
        try
            cfg.port = value.to!ushort;
        catch (Exception)
            return false;
        return true;
    case "appendonly":
        if (value == "yes")
            cfg.appendonly = true;
        else if (value == "no")
            cfg.appendonly = false;
        else
            return false;
        return true;
    case "appendfilename":
        cfg.appendfilename = value.unquote;
        return true;
    case "dir":
        cfg.dir = value.unquote;
        return true;
    case "maxmemory":
        return parseMemory(value, cfg.maxmemory);
    case "maxmemory-policy":
        switch (value)
        {
        case "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random":
            cfg.maxmemoryPolicy = value;
            return true;
        default:
            return false;
        }
    default:
        return false;
    }
}

/// Loads a config file. Returns false when the file cannot be read or a
/// directive is invalid; unknownOut collects unknown directive names.
public bool loadConfig(string path, ref Config cfg, void delegate(string line) onWarn = null)
{
    import std.algorithm : splitter;
    import std.file : readText;
    import std.string : indexOf;

    string text;
    try
        text = path.readText;
    catch (Exception)
        return false;

    foreach (rawLine; text.splitter('\n'))
    {
        auto line = rawLine.strip;
        if (line.length == 0 || line[0] == '#')
            continue;
        auto sp = line.indexOf(' ');
        auto tab = line.indexOf('\t');
        if (tab >= 0 && (sp < 0 || tab < sp))
            sp = tab;
        string name = sp < 0 ? line : line[0 .. sp];
        string value = sp < 0 ? "" : line[sp + 1 .. $].strip.idup;
        if (!applyDirective(name.idup, value, cfg) && onWarn !is null)
            onWarn(line.idup);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    @("config.parse_file")
    unittest
    {
        import core.stdc.stdio : remove;
        import std.file : write;

        enum path = "/tmp/dreads_test.conf";
        path.write("# comment\n\nport 6444\nappendonly yes\n"
                ~ "appendfilename \"meu.aof\"\nmaxmemory 100mb\n"
                ~ "maxmemory-policy allkeys-lru\nunknown-thing 42\n");
        scope (exit)
            remove(path);

        Config cfg;
        string[] warned;
        loadConfig(path, cfg, (l) { warned ~= l; }).expect.to.equal(true);
        cfg.port.expect.to.equal(6444);
        cfg.appendonly.expect.to.equal(true);
        cfg.appendfilename.expect.to.equal("meu.aof");
        cfg.maxmemory.expect.to.equal(100UL * 1024 * 1024);
        cfg.maxmemoryPolicy.expect.to.equal("allkeys-lru");
        warned.length.expect.to.equal(1);
        warned[0].expect.to.contain("unknown-thing");

        Config bad;
        loadConfig("/tmp/definitely-missing.conf", bad).expect.to.equal(false);
    }

    @("config.memory_sizes_and_robustness")
    unittest
    {
        ulong b;
        parseMemory("1024", b).expect.to.equal(true);
        b.expect.to.equal(1024);
        parseMemory("1kb", b).expect.to.equal(true);
        b.expect.to.equal(1024);
        parseMemory("2GB", b).expect.to.equal(true);
        b.expect.to.equal(2UL * 1024 * 1024 * 1024);
        parseMemory("1g", b).expect.to.equal(true);
        b.expect.to.equal(1_000_000_000);
        parseMemory("", b).expect.to.equal(false);
        parseMemory("abc", b).expect.to.equal(false);
        parseMemory("12xyz34", b).expect.to.equal(false);

        Config cfg;
        applyDirective("port", "99999", cfg).expect.to.equal(false); // > ushort
        applyDirective("appendonly", "talvez", cfg).expect.to.equal(false);
        applyDirective("maxmemory-policy", "yolo", cfg).expect.to.equal(false);
        applyDirective("nope", "x", cfg).expect.to.equal(false);
    }
}
