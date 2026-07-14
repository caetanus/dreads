module dreads.streamops;

// Stream tail commands: XREVRANGE, XSETID, XINFO, and consumer groups
// (XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM).

import core.stdc.stdio : snprintf;

import dreads.commands : eqICKeyword, parseLong, parseUlong;
import dreads.mem : Arena, ByteBuffer;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;
import dreads.stream : FieldPair, Group, StreamID, nowMs;

private void repWrongTypeS(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

private void repStreamId(ref ByteBuffer o, StreamID id) @nogc nothrow
{
    char[48] b = void;
    auto n = snprintf(b.ptr, b.length, "%llu-%llu", id.ms, id.seq);
    repBulk(o, b[0 .. n]);
}

private void repEntry(ref ByteBuffer o, StreamID id, const(FieldPair)[] pairs) @nogc nothrow
{
    repArrayHeader(o, 2);
    repStreamId(o, id);
    repArrayHeader(o, pairs.length * 2);
    foreach (ref p; pairs)
    {
        repBulk(o, p.field);
        repBulk(o, p.value);
    }
}

/// "ms" / "ms-seq" / "-" / "+" / "$" is handled by callers where relevant.
private bool parseId(scope const(char)[] s, ulong seqDefault, out StreamID id) @nogc nothrow
{
    size_t dash = size_t.max;
    foreach (i, c; s)
    {
        if (c == '-' && i > 0)
        {
            dash = i;
            break;
        }
    }
    ulong ms, seq;
    if (dash == size_t.max)
    {
        if (!parseUlong(s, ms))
            return false;
        id = StreamID(ms, seqDefault);
        return true;
    }
    if (!parseUlong(s[0 .. dash], ms))
        return false;
    if (!parseUlong(s[dash + 1 .. $], seq))
        return false;
    id = StreamID(ms, seq);
    return true;
}

private bool parseRangeId(scope const(char)[] s, bool isStart, out StreamID id) @nogc nothrow
{
    if (s == "-")
    {
        id = StreamID.minId;
        return true;
    }
    if (s == "+")
    {
        id = StreamID.maxId;
        return true;
    }
    return parseId(s, isStart ? 0 : ulong.max, id);
}

/// XREVRANGE key end start [COUNT n]
public void xrevrange(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    size_t limit = 0;
    if (args.length == 5 && eqICKeyword(args[3].str, "COUNT"))
    {
        long n;
        if (!parseLong(args[4].str, n) || n < 0)
        {
            repError(o, "ERR value is not an integer or out of range");
            return;
        }
        limit = cast(size_t) n;
    }
    else if (args.length != 3)
    {
        repError(o, "ERR syntax error");
        return;
    }
    StreamID start, end;
    if (!parseRangeId(args[1].str, false, end) || !parseRangeId(args[2].str, true, start))
    {
        repError(o, "ERR Invalid stream ID specified as stream command argument");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    if (obj is null)
    {
        repArrayHeader(o, 0);
        return;
    }
    // collect ascending, emit reversed with the count applied from the end
    struct Hit
    {
        StreamID id;
        const(FieldPair)[] pairs;
    }

    auto hits = arena.allocArray!Hit(obj.stream.length);
    size_t n = 0;
    obj.stream.walkRange(start, end, 0, (id, pairs) {
        hits[n++] = Hit(id, pairs);
        return 0;
    });
    auto take = limit && limit < n ? limit : n;
    repArrayHeader(o, take);
    foreach (k; 0 .. take)
        repEntry(o, hits[n - 1 - k].id, hits[n - 1 - k].pairs);
}

/// XSETID key id
public void xsetid(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'xsetid' command");
        return;
    }
    StreamID id;
    if (!parseId(args[1].str, 0, id))
    {
        repError(o, "ERR Invalid stream ID specified as stream command argument");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    if (obj is null)
    {
        repError(o, "ERR The XSETID command requires the key to exist.");
        return;
    }
    // must not be smaller than the current top entry
    bool bad = false;
    obj.stream.walkRange(StreamID.minId, StreamID.maxId, 0, (eid, pairs) {
        if (eid > id)
            bad = true;
        return 0;
    });
    if (bad)
    {
        repError(o,
                "ERR The ID specified in XSETID is smaller than the target stream top item");
        return;
    }
    obj.stream.lastId = id;
    notifyKeyspaceEvent(NClass.stream, "xsetid", args[0].str);
    repSimple(o, "OK");
}

/// XINFO STREAM key | XINFO GROUPS key
public void xinfo(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length != 2)
    {
        repError(o, "ERR syntax error");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[1].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    if (obj is null)
    {
        repError(o, "ERR no such key");
        return;
    }
    if (eqICKeyword(args[0].str, "STREAM"))
    {
        repArrayHeader(o, 8);
        repBulk(o, "length");
        repInt(o, cast(long) obj.stream.length);
        repBulk(o, "last-generated-id");
        repStreamId(o, obj.stream.lastId);
        repBulk(o, "groups");
        repInt(o, cast(long) obj.stream.groups.length);
        repBulk(o, "first-entry");
        bool emitted = false;
        obj.stream.walkRange(StreamID.minId, StreamID.maxId, 1, (id, pairs) {
            repEntry(o, id, pairs);
            emitted = true;
            return 0;
        });
        if (!emitted)
            repNullBulk(o);
        return;
    }
    if (eqICKeyword(args[0].str, "GROUPS"))
    {
        repArrayHeader(o, obj.stream.groups.length);
        foreach (i; 0 .. obj.stream.groups.capacity)
        {
            if (!obj.stream.groups.slotLive(i))
                continue;
            auto g = obj.stream.groups.valAt(i);
            repArrayHeader(o, 8);
            repBulk(o, "name");
            repBulk(o, obj.stream.groups.keyAt(i));
            repBulk(o, "consumers");
            repInt(o, cast(long) g.consumers.length);
            repBulk(o, "pending");
            repInt(o, cast(long) g.pending.length);
            repBulk(o, "last-delivered-id");
            repStreamId(o, g.lastDelivered);
        }
        return;
    }
    repUnknownSubcommand(o, "XINFO", args.length ? args[0].str : "");
}

// ---------------------------------------------------------------------------
// Consumer groups
// ---------------------------------------------------------------------------

private void repNoGroup(ref ByteBuffer o, scope const(char)[] group,
        scope const(char)[] key) @nogc nothrow
{
    o.append("-NOGROUP No such consumer group '");
    o.append(group);
    o.append("' for key name '");
    o.append(key);
    o.append("'\r\n");
}

/// XGROUP CREATE key g id|$ [MKSTREAM] | DESTROY key g |
/// CREATECONSUMER key g c | DELCONSUMER key g c
public void xgroup(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 3)
    {
        repError(o, "ERR wrong number of arguments for 'xgroup' command");
        return;
    }
    auto sub = args[0].str;
    auto key = args[1].str;
    auto gname = args[2].str;
    bool wrong;
    auto obj = ks.lookupTyped(key, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }

    if (eqICKeyword(sub, "CREATE"))
    {
        if (args.length < 4)
        {
            repError(o, "ERR wrong number of arguments for 'xgroup' command");
            return;
        }
        bool mkstream = args.length == 5 && eqICKeyword(args[4].str, "MKSTREAM");
        if (obj is null && !mkstream)
        {
            repError(o,
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.");
            return;
        }
        if (obj is null)
        {
            obj = ks.getOrCreate(key, ObjType.stream, wrong);
        }
        if (obj.stream.groups.exists(gname))
        {
            repError(o, "BUSYGROUP Consumer Group name already exists");
            return;
        }
        StreamID at;
        if (args[3].str == "$")
            at = obj.stream.lastId;
        else if (!parseId(args[3].str, 0, at))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        Group g;
        g.lastDelivered = at;
        obj.stream.groups.set(gname, g);
        repSimple(o, "OK");
        return;
    }
    if (eqICKeyword(sub, "DESTROY"))
    {
        repInt(o, obj !is null && obj.stream.groups.del(gname) ? 1 : 0);
        return;
    }
    if (eqICKeyword(sub, "SETID"))
    {
        // XGROUP SETID key group id|$ [ENTRIESREAD n] — n is accepted, unused
        if (args.length != 4 && !(args.length == 6 && eqICKeyword(args[4].str, "ENTRIESREAD")))
        {
            repError(o, "ERR wrong number of arguments for 'xgroup' command");
            return;
        }
        auto g = obj is null ? null : obj.stream.groups.get(gname);
        if (g is null)
        {
            repNoGroup(o, gname, key);
            return;
        }
        StreamID at;
        if (args[3].str == "$")
            at = obj.stream.lastId;
        else if (!parseId(args[3].str, 0, at))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        g.lastDelivered = at;
        repSimple(o, "OK");
        return;
    }
    if (eqICKeyword(sub, "CREATECONSUMER") || eqICKeyword(sub, "DELCONSUMER"))
    {
        if (args.length != 4)
        {
            repError(o, "ERR wrong number of arguments for 'xgroup' command");
            return;
        }
        auto g = obj is null ? null : obj.stream.groups.get(gname);
        if (g is null)
        {
            repNoGroup(o, gname, key);
            return;
        }
        auto cname = args[3].str;
        if (eqICKeyword(sub, "CREATECONSUMER"))
        {
            repInt(o, g.consumers.exists(cname) ? 0 : 1);
            g.ensureConsumer(cname);
            return;
        }
        // DELCONSUMER: drop its pending entries, reply how many
        long removed = 0;
        size_t i = 0;
        while (i < g.pending.length)
        {
            if (g.pending[i].consumer == cname)
            {
                g.pelRemove(g.pending[i].id);
                removed++;
            }
            else
                i++;
        }
        g.consumers.del(cname);
        repInt(o, removed);
        return;
    }
    repUnknownSubcommand(o, "XGROUP", sub);
}

/// XREADGROUP GROUP g c [COUNT n] [NOACK] STREAMS key [key...] id [id...]
public void xreadgroup(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 6 || !eqICKeyword(args[0].str, "GROUP"))
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto gname = args[1].str;
    auto cname = args[2].str;
    long count = 0;
    bool noack = false;
    size_t i = 3;
    while (i < args.length && !eqICKeyword(args[i].str, "STREAMS"))
    {
        if (eqICKeyword(args[i].str, "COUNT") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, count) || count < 0)
            {
                repError(o, "ERR value is not an integer or out of range");
                return;
            }
            i += 2;
        }
        else if (eqICKeyword(args[i].str, "NOACK"))
        {
            noack = true;
            i++;
        }
        else if (eqICKeyword(args[i].str, "BLOCK") && i + 1 < args.length)
        {
            // scripts/replay can never wait: validate, then one immediate
            // attempt (live connections park at the server layer instead)
            long ms;
            if (!parseLong(args[i + 1].str, ms) || ms < 0)
            {
                repError(o, "ERR timeout is not an integer or out of range");
                return;
            }
            i += 2;
        }
        else
        {
            repError(o, "ERR syntax error");
            return;
        }
    }
    if (i >= args.length)
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto rest = args[i + 1 .. $];
    if (rest.length == 0 || rest.length % 2 != 0)
    {
        repError(o,
                "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified.");
        return;
    }
    auto half = rest.length / 2;

    // validate everything upfront
    foreach (k; 0 .. half)
    {
        bool wrong;
        auto obj = ks.lookupTyped(rest[k].str, ObjType.stream, wrong);
        if (wrong)
        {
            repWrongTypeS(o);
            return;
        }
        if (obj is null || !obj.stream.groups.exists(gname))
        {
            repNoGroup(o, gname, rest[k].str);
            return;
        }
        StreamID ignored;
        if (rest[half + k].str != ">" && !parseId(rest[half + k].str, 0, ignored))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
    }

    auto now = nowMs();
    // first pass: which streams produce data?
    size_t withData = 0;
    foreach (k; 0 .. half)
    {
        auto obj = ks.lookup(rest[k].str);
        auto g = obj.stream.groups.get(gname);
        if (rest[half + k].str == ">")
        {
            bool ok;
            obj.stream.nextAfter(g.lastDelivered, ok);
            if (ok)
                withData++;
        }
        else
        {
            // pending re-delivery always answers (possibly an empty list)
            withData++;
        }
    }
    if (withData == 0)
    {
        repNullArray(o);
        return;
    }
    repArrayHeader(o, withData);
    foreach (k; 0 .. half)
    {
        auto obj = ks.lookup(rest[k].str);
        auto g = obj.stream.groups.get(gname);
        auto spec = rest[half + k].str;
        if (spec == ">")
        {
            bool ok;
            obj.stream.nextAfter(g.lastDelivered, ok);
            if (!ok)
                continue;
            auto consumer = g.ensureConsumer(cname);
            // count deliverable first
            size_t n = 0;
            auto cur = g.lastDelivered;
            for (;;)
            {
                bool more;
                auto id = obj.stream.nextAfter(cur, more);
                if (!more || (count && n == cast(size_t) count))
                    break;
                n++;
                cur = id;
            }
            repArrayHeader(o, 2);
            repBulk(o, rest[k].str);
            repArrayHeader(o, n);
            foreach (_; 0 .. n)
            {
                bool more;
                auto id = obj.stream.nextAfter(g.lastDelivered, more);
                obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; });
                g.lastDelivered = id;
                if (!noack)
                    g.pelSet(id, consumer, now, 1);
            }
        }
        else
        {
            StreamID after;
            parseId(spec, 0, after);
            // this consumer's pending entries with id > after, still existing
            size_t n = 0;
            foreach (ref pe; g.pending)
            {
                if (pe.id > after && pe.consumer == cname
                        && obj.stream.getEntry(pe.id, (pairs) => 0) == 0)
                {
                    n++;
                    if (count && n == cast(size_t) count)
                        break;
                }
            }
            repArrayHeader(o, 2);
            repBulk(o, rest[k].str);
            repArrayHeader(o, n);
            size_t emitted = 0;
            foreach (ref pe; g.pending)
            {
                if (emitted == n)
                    break;
                if (pe.id > after && pe.consumer == cname)
                {
                    auto id = pe.id;
                    if (obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; }) == 0)
                        emitted++;
                }
            }
        }
    }
}

/// XACK key group id [id ...]
public void xack(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 3)
    {
        repError(o, "ERR wrong number of arguments for 'xack' command");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    auto g = obj is null ? null : obj.stream.groups.get(args[1].str);
    if (g is null)
    {
        repInt(o, 0);
        return;
    }
    long n = 0;
    foreach (ref a; args[2 .. $])
    {
        StreamID id;
        if (!parseId(a.str, 0, id))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        n += g.pelRemove(id) ? 1 : 0;
    }
    repInt(o, n);
}

/// XPENDING key group  (summary)  |  XPENDING key group start end count [consumer]
public void xpending(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'xpending' command");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    auto g = obj is null ? null : obj.stream.groups.get(args[1].str);
    if (g is null)
    {
        repNoGroup(o, args[1].str, args[0].str);
        return;
    }
    if (args.length == 2) // summary form
    {
        repArrayHeader(o, 4);
        repInt(o, cast(long) g.pending.length);
        if (g.pending.length == 0)
        {
            repNullBulk(o);
            repNullBulk(o);
            repNullArray(o);
            return;
        }
        repStreamId(o, g.pending[0].id);
        repStreamId(o, g.pending[$ - 1].id);
        // per-consumer counts
        size_t nconsumers = 0;
        foreach (ci; 0 .. g.consumers.capacity)
        {
            if (!g.consumers.slotLive(ci))
                continue;
            foreach (ref pe; g.pending)
            {
                if (pe.consumer == g.consumers.keyAt(ci))
                {
                    nconsumers++;
                    break;
                }
            }
        }
        repArrayHeader(o, nconsumers);
        foreach (ci; 0 .. g.consumers.capacity)
        {
            if (!g.consumers.slotLive(ci))
                continue;
            long cnt = 0;
            foreach (ref pe; g.pending)
            {
                if (pe.consumer == g.consumers.keyAt(ci))
                    cnt++;
            }
            if (cnt == 0)
                continue;
            repArrayHeader(o, 2);
            repBulk(o, g.consumers.keyAt(ci));
            char[24] b = void;
            auto bn = snprintf(b.ptr, b.length, "%lld", cnt);
            repBulk(o, b[0 .. bn]);
        }
        return;
    }
    // extended form
    if (args.length < 5)
    {
        repError(o, "ERR syntax error");
        return;
    }
    StreamID start, end;
    long count;
    if (!parseRangeId(args[2].str, true, start) || !parseRangeId(args[3].str, false, end)
            || !parseLong(args[4].str, count) || count < 0)
    {
        repError(o, "ERR syntax error");
        return;
    }
    const(char)[] onlyConsumer = args.length == 6 ? args[5].str : null;
    auto now = nowMs();
    size_t n = 0;
    foreach (ref pe; g.pending)
    {
        if (pe.id < start || pe.id > end)
            continue;
        if (onlyConsumer !is null && pe.consumer != onlyConsumer)
            continue;
        if (n == cast(size_t) count)
            break;
        n++;
    }
    repArrayHeader(o, n);
    size_t emitted = 0;
    foreach (ref pe; g.pending)
    {
        if (emitted == n)
            break;
        if (pe.id < start || pe.id > end)
            continue;
        if (onlyConsumer !is null && pe.consumer != onlyConsumer)
            continue;
        repArrayHeader(o, 4);
        repStreamId(o, pe.id);
        repBulk(o, pe.consumer);
        repInt(o, cast(long)(now > pe.deliveryTimeMs ? now - pe.deliveryTimeMs : 0));
        repInt(o, cast(long) pe.deliveryCount);
        emitted++;
    }
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]
public void xautoclaim(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 5)
    {
        repError(o, "ERR wrong number of arguments for 'xautoclaim' command");
        return;
    }
    long minIdle, count = 100;
    StreamID start;
    if (!parseLong(args[3].str, minIdle) || minIdle < 0)
    {
        repError(o, "ERR Invalid min-idle-time argument for XAUTOCLAIM");
        return;
    }
    if (!parseRangeId(args[4].str, true, start))
    {
        repError(o, "ERR Invalid stream ID specified as stream command argument");
        return;
    }
    bool justId;
    size_t i = 5;
    while (i < args.length)
    {
        if (eqICKeyword(args[i].str, "COUNT") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, count) || count < 1)
            {
                repError(o, "ERR COUNT must be > 0");
                return;
            }
            i += 2;
        }
        else if (eqICKeyword(args[i].str, "JUSTID"))
        {
            justId = true;
            i++;
        }
        else
        {
            repError(o, "ERR syntax error");
            return;
        }
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    auto g = obj is null ? null : obj.stream.groups.get(args[1].str);
    if (g is null)
    {
        repNoGroup(o, args[1].str, args[0].str);
        return;
    }
    auto now = nowMs();
    auto consumer = g.ensureConsumer(args[2].str);
    // collect claimable ids (arena copies: pelSet mutates the list)
    auto claimable = arena.allocArray!StreamID(g.pending.length);
    size_t n = 0;
    StreamID nextCursor = StreamID(0, 0);
    foreach (ref pe; g.pending)
    {
        if (pe.id < start)
            continue;
        if (n == cast(size_t) count)
        {
            nextCursor = pe.id;
            break;
        }
        auto idle = now > pe.deliveryTimeMs ? now - pe.deliveryTimeMs : 0;
        if (idle < cast(ulong) minIdle)
            continue;
        if (obj.stream.getEntry(pe.id, (pairs) => 0) < 0)
            continue; // deleted entries are skipped
        claimable[n++] = pe.id;
    }
    repArrayHeader(o, 3);
    repStreamId(o, nextCursor);
    repArrayHeader(o, n);
    foreach (id; claimable[0 .. n])
    {
        auto pi = g.pelFind(id);
        auto dc = g.pending[pi].deliveryCount + (justId ? 0 : 1);
        g.pelSet(id, consumer, now, dc);
        if (justId)
            repStreamId(o, id);
        else
            obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; });
    }
    repArrayHeader(o, 0); // deleted-and-acked ids: we skip instead
}

/// XCLAIM key group consumer min-idle-time id [id ...] [JUSTID]
public void xclaim(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 5)
    {
        repError(o, "ERR wrong number of arguments for 'xclaim' command");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
    if (wrong)
    {
        repWrongTypeS(o);
        return;
    }
    auto g = obj is null ? null : obj.stream.groups.get(args[1].str);
    if (g is null)
    {
        repNoGroup(o, args[1].str, args[0].str);
        return;
    }
    long minIdle;
    if (!parseLong(args[3].str, minIdle) || minIdle < 0)
    {
        repError(o, "ERR Invalid min-idle-time argument for XCLAIM");
        return;
    }
    auto ids = args[4 .. $];
    bool justId = ids.length > 0 && eqICKeyword(ids[$ - 1].str, "JUSTID");
    if (justId)
        ids = ids[0 .. $ - 1];
    auto now = nowMs();
    auto consumer = g.ensureConsumer(args[2].str);
    // count claimable
    size_t n = 0;
    foreach (ref a; ids)
    {
        StreamID id;
        if (!parseId(a.str, 0, id))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        auto pi = g.pelFind(id);
        if (pi < 0)
            continue;
        auto idle = now > g.pending[pi].deliveryTimeMs ? now - g.pending[pi].deliveryTimeMs : 0;
        if (idle < cast(ulong) minIdle)
            continue;
        if (obj.stream.getEntry(id, (pairs) => 0) < 0)
            continue; // deleted entries are skipped (and not claimed)
        n++;
    }
    repArrayHeader(o, n);
    foreach (ref a; ids)
    {
        StreamID id;
        parseId(a.str, 0, id);
        auto pi = g.pelFind(id);
        if (pi < 0)
            continue;
        auto idle = now > g.pending[pi].deliveryTimeMs ? now - g.pending[pi].deliveryTimeMs : 0;
        if (idle < cast(ulong) minIdle)
            continue;
        if (obj.stream.getEntry(id, (pairs) => 0) < 0)
            continue;
        auto dc = g.pending[pi].deliveryCount + (justId ? 0 : 1);
        g.pelSet(id, consumer, now, dc);
        if (justId)
            repStreamId(o, id);
        else
            obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; });
    }
}
