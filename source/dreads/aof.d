module dreads.aof;

// Append-only file, phase 1. Write commands are logged as their original RESP
// bytes (zero-copy from the connection buffer), staged in a malloc'd buffer,
// fwrite+fflush'd once per network batch and fsync'd at most once per second
// (Redis's "everysec" policy). On boot the file is replayed through the same
// parser and dispatch that serve sockets.

import core.stdc.stdio : FILE, fclose, fopen, fflush, fread, fwrite, fprintf, stderr;

version (Posix)
{
    import core.sys.posix.stdio : fileno;
    import core.sys.posix.unistd : fsync;
}
version (Windows)
{
    import core.stdc.stdio : _fileno;

    extern (C) int _commit(int fd) nothrow @nogc;

    private alias fileno = _fileno;
    private alias fsync = _commit;
}

import dreads.commands : dispatch;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace;
import dreads.resp;
import dreads.scripting : evalCommand;

public struct Aof
{
    private FILE* f;
    private ByteBuffer pending;
    private bool dirty;

    @property bool enabled() const @nogc nothrow
    {
        return f !is null;
    }

    bool open(scope const(char)[] path) @nogc nothrow
    {
        char[512] zpath = void;
        if (path.length == 0 || path.length >= zpath.length)
            return false;
        zpath[0 .. path.length] = path;
        zpath[path.length] = 0;
        f = fopen(zpath.ptr, "ab");
        return f !is null;
    }

    void close() @nogc nothrow
    {
        if (f is null)
            return;
        flush();
        fsyncNow();
        fclose(f);
        f = null;
    }

    /// Stages one command's raw RESP bytes; cheap, no I/O.
    void append(scope const(ubyte)[] bytes) @nogc nothrow
    {
        if (f is null)
            return;
        pending.append(bytes);
    }

    /// Re-encodes EVALSHA as EVAL so replay does not depend on the script cache.
    /// rest = [numkeys, keys..., argv...].
    void appendEval(scope const(char)[] body_, const(RVal)[] rest) @nogc nothrow
    {
        if (f is null)
            return;
        repArrayHeader(pending, rest.length + 2);
        repBulk(pending, "EVAL");
        repBulk(pending, body_);
        foreach (ref a; rest)
            repBulk(pending, a.str);
    }

    /// Hands staged bytes to the OS (survives a process crash).
    void flush() @nogc nothrow
    {
        if (f is null || pending.empty)
            return;
        fwrite(pending.data.ptr, 1, pending.length, f);
        fflush(f);
        pending.clear();
        dirty = true;
    }

    /// Called by the 1s timer; the only place paying fsync latency.
    void fsyncNow() @nogc nothrow
    {
        if (f is null || !dirty)
            return;
        fsync(fileno(f));
        dirty = false;
    }
}

/// The server's logging policy, factored out so recovery tests exercise the
/// real thing: after dispatch, log the propagation override when a handler
/// set one, else the raw command when it is a write; errors are never logged.
/// Consumes (clears) the override either way.
public void logAfterDispatch(ref Aof aof, scope const(ubyte)[] rawCmd,
        scope const(char)[] unameUpper, scope const(ubyte)[] reply) nothrow
{
    import dreads.commands : isWriteCommand, propagationOverride;

    if (aof.enabled && reply.length > 0 && reply[0] != '-')
    {
        if (!propagationOverride.empty)
            aof.append(propagationOverride.data);
        else if (isWriteCommand(unameUpper))
            aof.append(rawCmd);
    }
    propagationOverride.clear();
}

/// Replays one logged command. EVAL goes to the scripting engine; everything
/// else through the regular dispatch.
private void replayCommand(const ref RVal cmd, ref Keyspace ks, ref ByteBuffer sink, ref Arena arena) nothrow
{
    if (cmd.type == RType.Array && cmd.arr.length > 0 && cmd.arr[0].type == RType.BulkString)
    {
        auto name = cmd.arr[0].str;
        if (name.length == 4)
        {
            char[4] up = void;
            foreach (i, c; name)
                up[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
            if (up == "EVAL")
            {
                evalCommand(cmd.arr[1 .. $], ks, sink, arena, false);
                return;
            }
        }
    }
    dispatch(cmd, ks, sink, arena);
}

/// Loads an AOF into ks. Returns the number of commands replayed, or -1 when
/// the file exists but is unreadable. A truncated tail is tolerated (warned).
public long aofLoad(scope const(char)[] path, ref Keyspace ks) nothrow
{
    char[512] zpath = void;
    if (path.length == 0 || path.length >= zpath.length)
        return -1;
    zpath[0 .. path.length] = path;
    zpath[path.length] = 0;
    auto f = fopen(zpath.ptr, "rb");
    if (f is null)
        return 0; // nothing to replay yet

    ByteBuffer inb;
    ByteBuffer sink;
    Arena arena;
    long count = 0;
    bool corrupt = false;

    for (;;)
    {
        auto space = inb.freeSpace(64 * 1024);
        auto n = fread(space.ptr, 1, space.length, f);
        if (n == 0)
            break;
        inb.grow(n);

        size_t pos = 0;
        for (;;)
        {
            RVal cmd;
            auto st = parseValue(inb.data, pos, arena, cmd);
            if (st == ParseStatus.incomplete)
                break;
            if (st == ParseStatus.protocolError)
            {
                corrupt = true;
                break;
            }
            replayCommand(cmd, ks, sink, arena);
            sink.clear();
            arena.reset();
            // replayed handlers (PEXPIREAT, XADD, ...) write the propagation
            // override; a stale one must never leak into post-boot logging
            {
                import dreads.commands : propagationOverride;

                propagationOverride.clear();
            }
            count++;
        }
        inb.consume(pos);
        if (corrupt)
            break;
    }
    fclose(f);

    if (corrupt)
        fprintf(stderr, "dreads: AOF corrupt after %lld commands; stopped replay\n", count);
    else if (!inb.empty)
        fprintf(stderr, "dreads: AOF has a truncated trailing command (%zu bytes ignored)\n",
                inb.length);
    return count;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import core.stdc.stdio : remove;

    private string respCmd(string[] args...)
    {
        import std.conv : to;

        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
        return r;
    }

    private string runOne(ref Keyspace ks, string[] args...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        auto encoded = respCmd(args);
        assert(parseValue(cast(const(ubyte)[]) encoded, pos, arena, v) == ParseStatus.ok);
        dispatch(v, ks, o, arena);
        return (cast(string) o.data).idup;
    }

    private void rmPath(string path)
    {
        remove((path ~ "\0").ptr);
    }
}

unittest // roundtrip: log writes, replay into a fresh keyspace
{
    enum path = "/tmp/dreads_aof_test_roundtrip.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "k", "v1"));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "k", "v2")); // overwrite wins
    aof.append(cast(const(ubyte)[]) respCmd("RPUSH", "l", "a", "b"));
    aof.append(cast(const(ubyte)[]) respCmd("ZADD", "z", "1.5", "m"));
    aof.append(cast(const(ubyte)[]) respCmd("SET", "gone", "x"));
    aof.append(cast(const(ubyte)[]) respCmd("DEL", "gone"));
    aof.flush();
    aof.fsyncNow();
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 6);
    assert(runOne(ks, "GET", "k") == "$2\r\nv2\r\n");
    assert(runOne(ks, "LRANGE", "l", "0", "-1") == "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    assert(runOne(ks, "ZSCORE", "z", "m") == "$3\r\n1.5\r\n");
    assert(runOne(ks, "EXISTS", "gone") == ":0\r\n");
}

unittest // missing file is fine; truncated tail is tolerated
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad("/tmp/dreads_aof_test_missing.aof", ks) == 0);

    enum path = "/tmp/dreads_aof_test_trunc.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);
    Aof aof;
    assert(aof.open(path));
    auto full = respCmd("SET", "ok", "yes");
    auto partial = respCmd("SET", "cut", "off");
    aof.append(cast(const(ubyte)[])(full ~ partial[0 .. partial.length - 7]));
    aof.close();

    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "ok") == "$3\r\nyes\r\n");
    assert(runOne(ks, "EXISTS", "cut") == ":0\r\n");
}

unittest // EVAL is replayed through the scripting engine
{
    enum path = "/tmp/dreads_aof_test_eval.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("EVAL",
            "redis.call('SET', KEYS[1], 'fromlua')", "1", "k2"));
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "k2") == "$7\r\nfromlua\r\n");
}

unittest // a translated XADD (resolved ID) replays with the identical ID
{
    enum path = "/tmp/dreads_aof_test_xadd.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    aof.append(cast(const(ubyte)[]) respCmd("XADD", "st", "1234-7", "f", "v"));
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "XLEN", "st") == ":1\r\n");
    auto entries = runOne(ks, "XRANGE", "st", "-", "+");
    import std.algorithm : canFind;

    assert(entries.canFind("1234-7")); // same ID, not a re-generated one
}

unittest // appendEval writes an EVAL the loader accepts
{
    enum path = "/tmp/dreads_aof_test_evalsha.aof";
    rmPath(path);
    scope (exit)
        rmPath(path);

    Aof aof;
    assert(aof.open(path));
    RVal[3] rest;
    rest[0].type = RType.BulkString;
    rest[0].str = "1";
    rest[1].type = RType.BulkString;
    rest[1].str = "k3";
    rest[2].type = RType.BulkString;
    rest[2].str = "value!";
    aof.appendEval("redis.call('SET', KEYS[1], ARGV[1])", rest[]);
    aof.close();

    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(aofLoad(path, ks) == 1);
    assert(runOne(ks, "GET", "k3") == "$6\r\nvalue!\r\n");
}
