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
    string rdbVersionCheck = "strict"; // strict | relaxed — RESTORE future-RDB-version policy
    long luaTimeLimitMs = 5000; // script execution budget; 0 = unlimited
    ulong luaMemoryLimit = 0; // bytes the Lua state may allocate; 0 = unlimited
    // SQLite-style durability: full = fsync per mutation (Raft-correct),
    // normal = flush per batch + periodic fsync, off = OS-buffered only
    string synchronous = "full";
    // Group-commit fsync backend for the Raft log: "threadpool" (a dedicated
    // OS thread running blocking fdatasync — the portable Posix default) or
    // "io_uring" (Linux: submit the fdatasync to a ring on that same thread).
    string fsyncBackend = "threadpool";
    // replication (Raft phase 1)
    uint raftNodeId = 0; // 0 = replication disabled
    string raftPeers; // "2@host:port,3@host:port"
    ushort raftPort = 0; // 0 = port + 10000
    bool raftJoin = false; // start as a passive learner (joining an existing cluster)
    // sharding (phase 2a): static slot-range topology. Each node owns a slot
    // range and MOVED-redirects the rest. cluster-nodes lists the whole map:
    // "lo-hi@host:port,lo-hi@host:port,..."; this node is the entry whose
    // host:port matches its own (matched by port on 127.0.0.1 for local runs).
    bool clusterEnabled = false;
    string clusterNodes;
    // keyspace notifications: flag string like "KEA" ("" = disabled). See notify.d.
    string notifyKeyspaceEvents;
    // active expiration: run the drop-soon timer that proactively reclaims keys
    // past their deadline. Off (default) = lazy-only expiry — a key is dropped
    // when next accessed (and KEYS/SCAN skip it), which keeps the SET-with-TTL
    // hot path free of index maintenance. On = bounded memory for TTL-heavy
    // workloads that never re-touch their keys, at a per-SET-EX cost.
    bool activeExpire = false;
    // opt-in background eviction cycle (default off): evict keys over maxmemory on
    // the timer, not only on writes. Off = write-triggered eviction only.
    bool activeEviction = false;
    // Data-structure encoding thresholds. dreads uses a single encoding per
    // type, so these are advisory: they are accepted, stored and echoed so the
    // Redis/Valkey test suite's CONFIG SET/GET round-trips (and encoding
    // fixtures) work. Defaults mirror Redis. `-ziplist-` names alias these.
    long hashMaxListpackEntries = 128;
    long hashMaxListpackValue = 64;
    long listMaxListpackSize = 128;
    long listCompressDepth = 0;
    long setMaxIntsetEntries = 512;
    long setMaxListpackEntries = 128;
    long setMaxListpackValue = 64;
    long zsetMaxListpackEntries = 128;
    long zsetMaxListpackValue = 64;
    long streamNodeMaxEntries = 100;
    ulong streamNodeMaxBytes = 4096;
    ulong protoMaxBulkLen = 512UL * 1024 * 1024;
    ulong clientQueryBufferLimit = 1024UL * 1024 * 1024;
    bool lazyfreeLazyServerDel = false;
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
    case "active-expire":
        if (value == "yes")
            cfg.activeExpire = true;
        else if (value == "no")
            cfg.activeExpire = false;
        else
            return false;
        return true;
    case "active-eviction":
        if (value == "yes")
            cfg.activeEviction = true;
        else if (value == "no")
            cfg.activeEviction = false;
        else
            return false;
        return true;
    case "dir":
        cfg.dir = value.unquote;
        return true;
    case "maxmemory":
        return parseMemory(value, cfg.maxmemory);
    case "maxmemory-policy":
        switch (value)
        {
        case "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random",
            "allkeys-lfu", "volatile-lfu", "volatile-random", "volatile-ttl":
            cfg.maxmemoryPolicy = value;
            return true;
        default:
            return false;
        }
    case "lua-time-limit":
        try
            cfg.luaTimeLimitMs = value.to!long;
        catch (Exception)
            return false;
        return cfg.luaTimeLimitMs >= 0;
    case "lua-memory-limit":
        return parseMemory(value, cfg.luaMemoryLimit);
    case "synchronous":
        switch (value)
        {
        case "off", "normal", "full":
            cfg.synchronous = value;
            return true;
        default:
            return false;
        }
    case "fsync-backend":
        switch (value)
        {
        case "threadpool", "io_uring":
            cfg.fsyncBackend = value;
            return true;
        default:
            return false;
        }
    case "raft-node-id":
        try
            cfg.raftNodeId = value.to!uint;
        catch (Exception)
            return false;
        return true;
    case "raft-peers":
        cfg.raftPeers = value.unquote;
        return true;
    case "raft-port":
        try
            cfg.raftPort = value.to!ushort;
        catch (Exception)
            return false;
        return true;
    case "raft-join":
        if (value == "yes")
            cfg.raftJoin = true;
        else if (value == "no")
            cfg.raftJoin = false;
        else
            return false;
        return true;
    case "cluster-enabled":
        if (value == "yes")
            cfg.clusterEnabled = true;
        else if (value == "no")
            cfg.clusterEnabled = false;
        else
            return false;
        return true;
    case "cluster-nodes":
        cfg.clusterNodes = value.unquote;
        return true;
    case "notify-keyspace-events":
        cfg.notifyKeyspaceEvents = value.unquote;
        return true;
        // encoding thresholds (advisory; stored for CONFIG round-trips).
    case "hash-max-listpack-entries", "hash-max-ziplist-entries":
        return parseLongCfg(value, cfg.hashMaxListpackEntries);
    case "hash-max-listpack-value", "hash-max-ziplist-value":
        return parseLongCfg(value, cfg.hashMaxListpackValue);
    case "list-max-listpack-size", "list-max-ziplist-size":
        return parseLongCfg(value, cfg.listMaxListpackSize);
    case "list-compress-depth":
        return parseLongCfg(value, cfg.listCompressDepth);
    case "set-max-intset-entries":
        return parseLongCfg(value, cfg.setMaxIntsetEntries);
    case "set-max-listpack-entries":
        return parseLongCfg(value, cfg.setMaxListpackEntries);
    case "set-max-listpack-value":
        return parseLongCfg(value, cfg.setMaxListpackValue);
    case "zset-max-listpack-entries", "zset-max-ziplist-entries":
        return parseLongCfg(value, cfg.zsetMaxListpackEntries);
    case "zset-max-listpack-value", "zset-max-ziplist-value":
        return parseLongCfg(value, cfg.zsetMaxListpackValue);
    case "stream-node-max-entries":
        return parseLongCfg(value, cfg.streamNodeMaxEntries);
    case "stream-node-max-bytes":
        return parseMemory(value, cfg.streamNodeMaxBytes);
    case "proto-max-bulk-len":
        return parseMemory(value, cfg.protoMaxBulkLen);
    case "client-query-buffer-limit":
        return parseMemory(value, cfg.clientQueryBufferLimit);
    case "lazyfree-lazy-server-del":
        if (value == "yes")
            cfg.lazyfreeLazyServerDel = true;
        else if (value == "no")
            cfg.lazyfreeLazyServerDel = false;
        else
            return false;
        return true;
    case "rdb-version-check": // strict rejects a future RDB version; relaxed accepts it
        if (value != "relaxed" && value != "strict")
            return false;
        cfg.rdbVersionCheck = value;
        return true;
    case "hll-sparse-max-bytes": // accepted: HLL sparse->dense threshold (informational)
        {
            long v;
            try
                v = value.to!long;
            catch (Exception)
                return false;
            return v >= 0;
        }
    case "import-mode":
        if (value != "yes" && value != "no")
            return false;
        import dreads.obj : gImportMode;

        gImportMode = value == "yes"; // pauses expiry while a bulk import runs
        return true;
        // accepted no-ops: dreads is never a replica, so replica flags are inert
        // but the test suite flips them in start_server overrides.
    case "replica-read-only", "slave-read-only":
        return value == "yes" || value == "no";
    case "save": // RDB snapshotting is not implemented; accept & ignore
        return true;
    case "aof-use-rdb-preamble": // dreads' AOF is its own format; RDB preamble is inert
        return value == "yes" || value == "no";
    case "hz": // background-task frequency; dreads uses a fixed timer
        try
            return value.to!int >= 0;
        catch (Exception)
            return false;
    case "appendfsync":
        return value == "always" || value == "everysec" || value == "no";
    case "repl-ping-replica-period", "repl-ping-slave-period":
        try
            return value.to!int >= 0;
        catch (Exception)
            return false;
    default:
        return false;
    }
}

private bool parseLongCfg(string value, ref long dst) nothrow
{
    import std.conv : to;

    try
        dst = value.to!long;
    catch (Exception)
        return false;
    return true;
}

/// Parameters that CONFIG SET may change at runtime. Startup-only settings
/// (port, dir, raft/cluster topology, appendonly) are excluded.
public bool isRuntimeSettable(string lname) nothrow
{
    switch (lname)
    {
    case "maxmemory", "maxmemory-policy", "lua-time-limit", "lua-memory-limit",
        "active-expire", "active-eviction", "notify-keyspace-events", "lazyfree-lazy-server-del",
        "appendonly", "import-mode", "replica-read-only", "slave-read-only",
        "save", "hz", "appendfsync",
        "repl-ping-replica-period", "repl-ping-slave-period",
        "hash-max-listpack-entries", "hash-max-ziplist-entries",
        "hash-max-listpack-value", "hash-max-ziplist-value",
        "list-max-listpack-size", "list-max-ziplist-size", "list-compress-depth",
        "set-max-intset-entries", "set-max-listpack-entries", "set-max-listpack-value",
        "zset-max-listpack-entries", "zset-max-ziplist-entries",
        "zset-max-listpack-value", "zset-max-ziplist-value",
        "stream-node-max-entries", "stream-node-max-bytes",
        "proto-max-bulk-len", "client-query-buffer-limit",
        "rdb-version-check", "hll-sparse-max-bytes", "aof-use-rdb-preamble":
        return true;
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
