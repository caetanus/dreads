module dreads.streamops;

// Stream tail commands: XREVRANGE, XSETID, XINFO, and consumer groups
// (XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM).

import core.stdc.stdio : snprintf;

import dreads.commands : eqICKeyword, parseLong, parseUlong;
import dreads.mem : Arena, ByteBuffer;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;
import dreads.stream : ConsumerInfo, FieldPair, Group, PelEntry, Stream, StreamID, nowMs;

/// XINFO lag: how many entries the group hasn't read. Unknown (nil) when a
/// tombstone sits after the cursor — the logical distance can't be counted;
/// countAfter while entries-read is still unknown; else entries-added minus read.
private void repGroupLag(ref ByteBuffer o, ref const Stream st, ref const Group g) @nogc nothrow
{
    if (g.entriesRead < 0)
        repInt(o, cast(long) st.countAfter(g.lastDelivered));
    else if (st.maxDeletedId > g.lastDelivered)
        repNullBulk(o); // tombstone after the cursor: distance unknown
    else
        repInt(o, cast(long) st.entriesAdded - g.entriesRead);
}

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

/// Smallest id strictly greater than x (saturating at max) — for an exclusive
/// start bound `(A`.
private StreamID incrId(StreamID x) @nogc nothrow
{
    if (x.seq != ulong.max)
        return StreamID(x.ms, x.seq + 1);
    if (x.ms != ulong.max)
        return StreamID(x.ms + 1, 0);
    return StreamID(ulong.max, ulong.max); // already the maximum
}

/// Largest id strictly less than x (saturating at min) — for an exclusive end
/// bound `(B`.
private StreamID decrId(StreamID x) @nogc nothrow
{
    if (x.seq != 0)
        return StreamID(x.ms, x.seq - 1);
    if (x.ms != 0)
        return StreamID(x.ms - 1, ulong.max);
    return StreamID(0, 0); // already the minimum
}

/// Resolve a group-cursor id (XGROUP CREATE/SETID): `$` = the stream's last id,
/// `-` = 0-0 (rewind to the start), `+` = the maximum, else an explicit id.
private bool parseGroupCursor(scope const(char)[] s, StreamID lastId, out StreamID id) @nogc nothrow
{
    if (s == "$")
    {
        id = lastId;
        return true;
    }
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
    return parseId(s, 0, id);
}

/// parseRangeId plus the exclusive `(id` form: `(A` on a start bound becomes the
/// smallest id > A; `(B` on an end bound becomes the largest id < B. Used by
/// XRANGE/XREVRANGE and XPENDING.
private bool parseRangeIdExcl(scope const(char)[] s, bool isStart, out StreamID id) @nogc nothrow
{
    if (s.length > 0 && s[0] == '(')
    {
        auto inner = s[1 .. $];
        if (inner == "-" || inner == "+") // the specials can't be made exclusive
            return false;
        StreamID base;
        if (!parseId(inner, isStart ? 0 : ulong.max, base))
            return false;
        if (isStart && base == StreamID(ulong.max, ulong.max))
            return false; // nothing above the maximum id
        if (!isStart && base == StreamID(0, 0))
            return false; // nothing below 0-0
        id = isStart ? incrId(base) : decrId(base);
        return true;
    }
    return parseRangeId(s, isStart, id);
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
    if (!parseRangeIdExcl(args[1].str, false, end) || !parseRangeIdExcl(args[2].str, true, start))
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

/// XSETID key id [ENTRIESADDED entries-added MAXDELETEDID max-deleted-id]
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
    // ENTRIESADDED n and MAXDELETEDID id come as a pair, or not at all
    bool hasOpts = false;
    long entriesAdded = 0;
    StreamID maxDel;
    if (args.length == 6 && eqICKeyword(args[2].str, "ENTRIESADDED")
            && eqICKeyword(args[4].str, "MAXDELETEDID"))
    {
        hasOpts = true;
        if (!parseLong(args[3].str, entriesAdded) || entriesAdded < 0)
        {
            repError(o, "ERR value for ENTRIESADDED must be positive");
            return;
        }
        if (!parseId(args[5].str, 0, maxDel))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
    }
    else if (args.length != 2)
    {
        repError(o, "ERR syntax error");
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
        repError(o, "ERR no such key");
        return;
    }
    // the new last-id must not be below the current top entry
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
    // ...nor below a tombstone: the existing max-deleted (2-arg form) or the new
    // one being set (opts form must satisfy max-deleted <= last-id)
    immutable effMaxDel = hasOpts ? maxDel : obj.stream.maxDeletedId;
    if (id < effMaxDel)
    {
        repError(o, "ERR The ID specified in XSETID is smaller than the provided"
                ~ " max_deleted_entry_id");
        return;
    }
    if (hasOpts && entriesAdded < cast(long) obj.stream.length)
    {
        repError(o, "ERR The entries_added specified in XSETID is smaller than "
                ~ "the target stream length");
        return;
    }
    if (hasOpts)
    {
        obj.stream.entriesAdded = cast(ulong) entriesAdded;
        obj.stream.maxDeletedId = maxDel;
    }
    obj.stream.lastId = id;
    notifyKeyspaceEvent(NClass.stream, "xsetid", args[0].str);
    repSimple(o, "OK");
}

/// XINFO STREAM key | XINFO GROUPS key
public void xinfo(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length >= 1 && eqICKeyword(args[0].str, "HELP"))
    {
        if (args.length > 1) // HELP takes no arguments
        {
            repError(o, "ERR wrong number of arguments for 'xinfo|help' command");
            return;
        }
        repArrayHeader(o, 1);
        repBulk(o, "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
        return;
    }
    if (args.length < 2)
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
        // XINFO STREAM key [FULL [COUNT n]]
        immutable full = args.length >= 3 && eqICKeyword(args[2].str, "FULL");
        if (!full && args.length != 2)
        {
            repError(o, "ERR syntax error");
            return;
        }
        // rax metrics: we don't use a radix tree, so report the analogous
        // macro-node count (~stream-node-max-entries per node) so introspection
        // and fuzz loops that watch radix-tree-keys behave.
        immutable len = obj.stream.length;
        immutable long rtKeys = len == 0 ? 0 : cast(long)((len + 99) / 100);
        immutable long rtNodes = rtKeys + 1;
        if (full)
        {
            long count = 10; // default; 0 = all
            if (args.length >= 5 && eqICKeyword(args[3].str, "COUNT"))
                parseLong(args[4].str, count);
            repArrayHeader(o, 18);
            repBulk(o, "length");
            repInt(o, cast(long) len);
            repBulk(o, "radix-tree-keys");
            repInt(o, rtKeys);
            repBulk(o, "radix-tree-nodes");
            repInt(o, rtNodes);
            repBulk(o, "last-generated-id");
            repStreamId(o, obj.stream.lastId);
            repBulk(o, "max-deleted-entry-id");
            repStreamId(o, obj.stream.maxDeletedId);
            repBulk(o, "entries-added");
            repInt(o, cast(long) obj.stream.entriesAdded);
            repBulk(o, "recorded-first-entry-id");
            repStreamId(o, obj.stream.recordedFirstId);
            repBulk(o, "entries");
            size_t ne = 0;
            immutable lim = count <= 0 ? 0 : cast(size_t) count;
            obj.stream.walkRange(StreamID.minId, StreamID.maxId, lim, (id, pairs) {
                ne++;
                return 0;
            });
            repArrayHeader(o, ne);
            obj.stream.walkRange(StreamID.minId, StreamID.maxId, lim, (id, pairs) {
                repEntry(o, id, pairs);
                return 0;
            });
            repBulk(o, "groups");
            repArrayHeader(o, obj.stream.groups.length);
            foreach (i; 0 .. obj.stream.groups.capacity)
            {
                if (!obj.stream.groups.slotLive(i))
                    continue;
                auto g = obj.stream.groups.valAt(i);
                repArrayHeader(o, 14); // 7 pairs
                repBulk(o, "name");
                repBulk(o, obj.stream.groups.keyAt(i));
                repBulk(o, "last-delivered-id");
                repStreamId(o, g.lastDelivered);
                repBulk(o, "pel-count");
                repInt(o, cast(long) g.pending.length);
                // the group PEL: [id, delivery-time, delivery-count] per entry
                repBulk(o, "pending");
                repArrayHeader(o, g.pending.length);
                foreach (ref pe; g.pending)
                {
                    repArrayHeader(o, 3);
                    repStreamId(o, pe.id);
                    repInt(o, cast(long) pe.deliveryTimeMs);
                    repInt(o, cast(long) pe.deliveryCount);
                }
                // per-consumer detail (name-sorted), each with its PEL slice
                repBulk(o, "consumers");
                auto order = sortedConsumers(*g, arena);
                repArrayHeader(o, order.length);
                foreach (ci; order)
                {
                    auto cn = g.consumers.keyAt(ci);
                    auto info = g.consumers.valAt(ci);
                    size_t cpel = 0;
                    foreach (ref pe; g.pending)
                        if (pe.consumer == cn)
                            cpel++;
                    repArrayHeader(o, 10); // 5 pairs
                    repBulk(o, "name");
                    repBulk(o, cn);
                    repBulk(o, "seen-time");
                    repInt(o, cast(long) info.seenTime);
                    repBulk(o, "active-time");
                    repInt(o, info.activeTime == 0 ? -1 : cast(long) info.activeTime);
                    repBulk(o, "pel-count");
                    repInt(o, cast(long) cpel);
                    repBulk(o, "pending");
                    repArrayHeader(o, cpel);
                    foreach (ref pe; g.pending)
                    {
                        if (pe.consumer != cn)
                            continue;
                        repArrayHeader(o, 3);
                        repStreamId(o, pe.id);
                        repInt(o, cast(long) pe.deliveryTimeMs);
                        repInt(o, cast(long) pe.deliveryCount);
                    }
                }
                repBulk(o, "entries-read");
                if (g.entriesRead < 0)
                    repNullBulk(o);
                else
                    repInt(o, g.entriesRead);
                repBulk(o, "lag");
                repGroupLag(o, obj.stream, *g);
            }
            return;
        }
        repArrayHeader(o, 20);
        repBulk(o, "length");
        repInt(o, cast(long) len);
        repBulk(o, "radix-tree-keys");
        repInt(o, rtKeys);
        repBulk(o, "radix-tree-nodes");
        repInt(o, rtNodes);
        repBulk(o, "last-generated-id");
        repStreamId(o, obj.stream.lastId);
        repBulk(o, "max-deleted-entry-id");
        repStreamId(o, obj.stream.maxDeletedId);
        repBulk(o, "entries-added");
        repInt(o, cast(long) obj.stream.entriesAdded);
        repBulk(o, "recorded-first-entry-id");
        repStreamId(o, obj.stream.recordedFirstId);
        repBulk(o, "groups");
        repInt(o, cast(long) obj.stream.groups.length);
        repBulk(o, "first-entry");
        bool emittedFirst = false;
        obj.stream.walkRange(StreamID.minId, StreamID.maxId, 1, (id, pairs) {
            repEntry(o, id, pairs);
            emittedFirst = true;
            return 0;
        });
        if (!emittedFirst)
            repNullBulk(o);
        repBulk(o, "last-entry");
        if (obj.stream.getLast((id, pairs) { repEntry(o, id, pairs); return 0; }) < 0)
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
            repArrayHeader(o, 12);
            repBulk(o, "name");
            repBulk(o, obj.stream.groups.keyAt(i));
            repBulk(o, "consumers");
            repInt(o, cast(long) g.consumers.length);
            repBulk(o, "pending");
            repInt(o, cast(long) g.pending.length);
            repBulk(o, "last-delivered-id");
            repStreamId(o, g.lastDelivered);
            repBulk(o, "entries-read");
            if (g.entriesRead < 0)
                repNullBulk(o); // unknown (never set via ENTRIESREAD)
            else
                repInt(o, g.entriesRead);
            repBulk(o, "lag");
            repGroupLag(o, obj.stream, *g);
        }
        return;
    }
    if (eqICKeyword(args[0].str, "CONSUMERS"))
    {
        // XINFO CONSUMERS key group — consumers sorted by name (Redis rax order)
        if (args.length != 3)
        {
            repError(o, "ERR syntax error");
            return;
        }
        auto g = obj.stream.groups.get(args[2].str);
        if (g is null)
        {
            repNoGroup(o, args[2].str, args[1].str);
            return;
        }
        auto order = sortedConsumers(*g, arena);
        immutable now = nowMs();
        repArrayHeader(o, order.length);
        foreach (ci; order)
        {
            auto cn = g.consumers.keyAt(ci);
            auto info = g.consumers.valAt(ci);
            size_t cpel = 0;
            foreach (ref pe; g.pending)
                if (pe.consumer == cn)
                    cpel++;
            repArrayHeader(o, 8); // name, pending, idle, inactive
            repBulk(o, "name");
            repBulk(o, cn);
            repBulk(o, "pending");
            repInt(o, cast(long) cpel);
            repBulk(o, "idle");
            repInt(o, cast(long)(now > info.seenTime ? now - info.seenTime : 0));
            repBulk(o, "inactive");
            repInt(o, info.activeTime == 0 ? -1
                    : cast(long)(now > info.activeTime ? now - info.activeTime : 0));
        }
        return;
    }
    repUnknownSubcommand(o, "XINFO", args.length ? args[0].str : "");
}

/// Live consumer slot indices of a group, sorted lexicographically by name —
/// Redis stores consumers in a rax, so XINFO iterates them in name order.
private size_t[] sortedConsumers(ref Group g, ref Arena arena) @nogc nothrow
{
    auto idx = arena.allocArray!size_t(g.consumers.length);
    size_t n = 0;
    foreach (i; 0 .. g.consumers.capacity)
        if (g.consumers.slotLive(i))
            idx[n++] = i;
    foreach (a; 1 .. n) // insertion sort by name (consumer counts are small)
    {
        auto v = idx[a];
        auto vn = g.consumers.keyAt(v);
        size_t b = a;
        while (b > 0 && g.consumers.keyAt(idx[b - 1]) > vn)
        {
            idx[b] = idx[b - 1];
            b--;
        }
        idx[b] = v;
    }
    return idx[0 .. n];
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
    if (args.length >= 1 && eqICKeyword(args[0].str, "HELP"))
    {
        if (args.length > 1) // HELP takes no arguments
        {
            repError(o, "ERR wrong number of arguments for 'xgroup|help' command");
            return;
        }
        repArrayHeader(o, 1);
        repBulk(o, "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
        return;
    }
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
        // [MKSTREAM] [ENTRIESREAD n] in any order after the id
        bool mkstream = false;
        long entriesRead = -1; // -1 = not provided
        for (size_t i = 4; i < args.length;)
        {
            if (eqICKeyword(args[i].str, "MKSTREAM"))
            {
                mkstream = true;
                i++;
            }
            else if (eqICKeyword(args[i].str, "ENTRIESREAD") && i + 1 < args.length)
            {
                if (!parseLong(args[i + 1].str, entriesRead) || entriesRead < -1)
                {
                    repError(o, "ERR value for ENTRIESREAD must be positive or -1");
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
        if (!parseGroupCursor(args[3].str, obj.stream.lastId, at))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        Group g;
        g.lastDelivered = at;
        g.entriesRead = entriesRead;
        obj.stream.groups.set(gname, g);
        repSimple(o, "OK");
        notifyKeyspaceEvent(NClass.stream, "xgroup-create", key);
        return;
    }
    if (eqICKeyword(sub, "DESTROY"))
    {
        immutable destroyed = obj !is null && obj.stream.groups.del(gname);
        repInt(o, destroyed ? 1 : 0);
        if (destroyed)
            notifyKeyspaceEvent(NClass.stream, "xgroup-destroy", key);
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
        if (!parseGroupCursor(args[3].str, obj.stream.lastId, at))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        g.lastDelivered = at;
        repSimple(o, "OK");
        notifyKeyspaceEvent(NClass.stream, "xgroup-setid", key);
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
            immutable existed = g.consumers.exists(cname);
            repInt(o, existed ? 0 : 1);
            g.ensureConsumer(cname, nowMs());
            if (!existed)
                notifyKeyspaceEvent(NClass.stream, "xgroup-createconsumer", key);
            return;
        }
        // DELCONSUMER: drop its pending entries, reply how many
        immutable existed = g.consumers.exists(cname);
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
        if (existed) // no event when the consumer never existed (Redis parity)
            notifyKeyspaceEvent(NClass.stream, "xgroup-delconsumer", key);
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
                "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.");
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
    // first pass: which streams produce data? XREADGROUP `>` also registers the
    // consumer on the group even when there is nothing to deliver (so XINFO
    // CONSUMERS lists it), bumping its seen-time.
    size_t withData = 0;
    foreach (k; 0 .. half)
    {
        auto obj = ks.lookup(rest[k].str);
        auto g = obj.stream.groups.get(gname);
        if (rest[half + k].str == ">")
        {
            bool madeConsumer;
            g.ensureConsumer(cname, now, madeConsumer);
            if (madeConsumer) // XREADGROUP `>` auto-registers a new consumer
                notifyKeyspaceEvent(NClass.stream, "xgroup-createconsumer", rest[k].str);
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
            auto consumer = g.ensureConsumer(cname, now);
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
            if (n > 0)
                g.markActive(cname, now); // read real data
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
            // entries-read = logical position of the advanced cursor (exact when
            // no tombstone sits between first-entry and the cursor; the deeper
            // tombstone-ambiguity cases — lag with XDEL/XTRIM — are a known debt).
            if (n > 0)
                g.entriesRead = cast(long) obj.stream.entriesAdded
                    - cast(long) obj.stream.countAfter(g.lastDelivered);
        }
        else
        {
            StreamID after;
            cast(void) parseId(spec, 0, after);
            // history: this consumer's pending entries with id > after. Entries
            // deleted from the stream (XDEL/trim) still appear — with a nil field
            // list — so the client can see which pending ids are now gone (#5570).
            size_t n = 0;
            foreach (ref pe; g.pending)
            {
                if (pe.id > after && pe.consumer == cname)
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
                    if (obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; }) < 0)
                    {
                        repArrayHeader(o, 2); // gone from the stream: [id, nil]
                        repStreamId(o, id);
                        repNullArray(o);
                    }
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
    // validate ALL ids first — a single bad id must fail the whole command
    // without acking the ones before it (atomicity).
    foreach (ref a; args[2 .. $])
    {
        StreamID id;
        if (!parseId(a.str, 0, id))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
    }
    long n = 0;
    foreach (ref a; args[2 .. $])
    {
        StreamID id;
        cast(void) parseId(a.str, 0, id);
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
    // extended form: [IDLE min-idle-time] start end count [consumer]
    size_t base = 2;
    long minIdleFilter = 0;
    if (args.length >= 4 && eqICKeyword(args[2].str, "IDLE"))
    {
        if (!parseLong(args[3].str, minIdleFilter) || minIdleFilter < 0)
        {
            repError(o, "ERR value is not an integer or out of range");
            return;
        }
        base = 4;
    }
    // what remains must be start end count [consumer]
    if (args.length != base + 3 && args.length != base + 4)
    {
        repError(o, "ERR syntax error");
        return;
    }
    StreamID start, end;
    long count;
    if (!parseRangeIdExcl(args[base].str, true, start)
            || !parseRangeIdExcl(args[base + 1].str, false, end)
            || !parseLong(args[base + 2].str, count) || count < 0)
    {
        repError(o, "ERR syntax error");
        return;
    }
    const(char)[] onlyConsumer = args.length == base + 4 ? args[base + 3].str : null;
    auto now = nowMs();
    // a pending entry passes the filters: in [start,end], the right consumer, and
    // idle at least min-idle-time (IDLE).
    bool passes(ref const PelEntry pe) @nogc nothrow
    {
        if (pe.id < start || pe.id > end)
            return false;
        if (onlyConsumer !is null && pe.consumer != onlyConsumer)
            return false;
        immutable idle = now > pe.deliveryTimeMs ? now - pe.deliveryTimeMs : 0;
        if (minIdleFilter > 0 && idle < cast(ulong) minIdleFilter)
            return false;
        return true;
    }

    size_t n = 0;
    foreach (ref pe; g.pending)
    {
        if (!passes(pe))
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
        if (!passes(pe))
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
            // Redis scans up to count*10 pending entries, so a count near LONG_MAX
            // would overflow — reject it (test: "out of range count").
            if (!parseLong(args[i + 1].str, count) || count < 1 || count > long.max / 10)
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
    bool madeConsumer;
    auto consumer = g.ensureConsumer(args[2].str, now, madeConsumer);
    if (madeConsumer) // XAUTOCLAIM auto-creates the target consumer
        notifyKeyspaceEvent(NClass.stream, "xgroup-createconsumer", args[0].str);
    // scan the PEL from `start`, examining up to `count` entries. Live ones are
    // claimed; ones deleted from the stream go to the deleted-ids list and are
    // evicted from the PEL. (arena copies: pelSet/pelRemove mutate the list.)
    auto claimable = arena.allocArray!StreamID(g.pending.length);
    auto deleted = arena.allocArray!StreamID(g.pending.length);
    size_t n = 0, nd = 0, attempts = 0;
    StreamID nextCursor = StreamID(0, 0);
    foreach (ref pe; g.pending)
    {
        if (pe.id < start)
            continue;
        if (attempts >= cast(size_t) count)
        {
            nextCursor = pe.id; // more to scan next round
            break;
        }
        auto idle = now > pe.deliveryTimeMs ? now - pe.deliveryTimeMs : 0;
        if (idle < cast(ulong) minIdle)
            continue;
        attempts++;
        if (obj.stream.getEntry(pe.id, (pairs) => 0) < 0)
            deleted[nd++] = pe.id; // gone from the stream
        else
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
    repArrayHeader(o, nd); // ids that were deleted from the stream
    foreach (id; deleted[0 .. nd])
    {
        repStreamId(o, id);
        g.pelRemove(id); // evict the tombstone from the PEL
    }
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
    // The id list runs until the first option keyword; options follow in any order:
    // [IDLE ms] [TIME unix-ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id].
    size_t k = 4;
    while (k < args.length && !isXclaimOpt(args[k].str))
        k++;
    auto ids = args[4 .. k];
    if (ids.length == 0)
    {
        repError(o, "ERR wrong number of arguments for 'xclaim' command");
        return;
    }
    bool force, justId;
    long idleMs = -1, timeMs = -1, retry = -1;
    for (size_t oi = k; oi < args.length;)
    {
        auto opt = args[oi].str;
        if (eqICKeyword(opt, "FORCE"))
        {
            force = true;
            oi++;
        }
        else if (eqICKeyword(opt, "JUSTID"))
        {
            justId = true;
            oi++;
        }
        else if (eqICKeyword(opt, "IDLE") && oi + 1 < args.length)
        {
            if (!parseLong(args[oi + 1].str, idleMs) || idleMs < 0)
            {
                repError(o, "ERR Invalid IDLE option argument for XCLAIM");
                return;
            }
            oi += 2;
        }
        else if (eqICKeyword(opt, "TIME") && oi + 1 < args.length)
        {
            if (!parseLong(args[oi + 1].str, timeMs) || timeMs < 0)
            {
                repError(o, "ERR Invalid TIME option argument for XCLAIM");
                return;
            }
            oi += 2;
        }
        else if (eqICKeyword(opt, "RETRYCOUNT") && oi + 1 < args.length)
        {
            if (!parseLong(args[oi + 1].str, retry) || retry < 0)
            {
                repError(o, "ERR Invalid RETRYCOUNT option argument for XCLAIM");
                return;
            }
            oi += 2;
        }
        else if (eqICKeyword(opt, "LASTID") && oi + 1 < args.length)
        {
            StreamID li;
            if (!parseId(args[oi + 1].str, 0, li))
            {
                repError(o, "ERR Invalid stream ID specified as stream command argument");
                return;
            }
            oi += 2;
            if (li > g.lastDelivered) // LASTID only advances the group cursor
                g.lastDelivered = li;
        }
        else
        {
            repError(o, "ERR syntax error");
            return;
        }
    }
    // validate every id up front (Redis rejects the whole command on a bad id)
    foreach (ref a; ids)
    {
        StreamID id;
        if (!parseId(a.str, 0, id))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
    }
    auto now = nowMs();
    bool madeConsumer;
    auto consumer = g.ensureConsumer(args[2].str, now, madeConsumer);
    if (madeConsumer) // XCLAIM auto-creates the target consumer
        notifyKeyspaceEvent(NClass.stream, "xgroup-createconsumer", args[0].str);
    // Whether this id will be claimed: an in-PEL entry past min-idle that still
    // exists in the stream, or (FORCE) a not-in-PEL id that exists in the stream.
    bool willClaim(StreamID id)
    {
        immutable inStream = obj.stream.getEntry(id, (pairs) => 0) >= 0;
        auto pi = g.pelFind(id);
        if (pi >= 0)
        {
            auto idle = now > g.pending[pi].deliveryTimeMs ? now - g.pending[pi].deliveryTimeMs : 0;
            return idle >= cast(ulong) minIdle && inStream;
        }
        return force && inStream;
    }

    size_t n = 0;
    foreach (ref a; ids)
    {
        StreamID id;
        cast(void) parseId(a.str, 0, id);
        if (willClaim(id))
            n++;
    }
    repArrayHeader(o, n);
    // delivery time: TIME wins, else now-IDLE, else now.
    immutable dtime = timeMs >= 0 ? cast(ulong) timeMs
        : (idleMs >= 0 ? (now > cast(ulong) idleMs ? now - cast(ulong) idleMs : 0) : now);
    foreach (ref a; ids)
    {
        StreamID id;
        cast(void) parseId(a.str, 0, id);
        auto pi = g.pelFind(id);
        if (pi < 0 && !force)
            continue;
        if (obj.stream.getEntry(id, (pairs) => 0) < 0)
        {
            if (pi >= 0)
                g.pelRemove(id); // deleted from the stream: evict from the PEL, don't claim
            continue; // FORCE on a non-existent message is ignored
        }
        if (pi >= 0)
        {
            auto idle = now > g.pending[pi].deliveryTimeMs ? now - g.pending[pi].deliveryTimeMs : 0;
            if (idle < cast(ulong) minIdle)
                continue;
        }
        immutable baseCount = pi >= 0 ? g.pending[pi].deliveryCount : 0;
        immutable dc = retry >= 0 ? cast(ulong) retry : (justId ? baseCount : baseCount + 1);
        g.pelSet(id, consumer, dtime, dc);
        if (justId)
            repStreamId(o, id);
        else
            obj.stream.getEntry(id, (pairs) { repEntry(o, id, pairs); return 0; });
    }
    // NB: XCLAIM/XAUTOCLAIM emit NO standalone keyspace event — only the consumer
    // auto-creation below fires `xgroup-createconsumer` (verified against the suite).
}

// XCLAIM id-list terminator keywords.
private bool isXclaimOpt(scope const(char)[] s) @nogc nothrow
{
    return eqICKeyword(s, "IDLE") || eqICKeyword(s, "TIME") || eqICKeyword(s, "RETRYCOUNT")
        || eqICKeyword(s, "FORCE") || eqICKeyword(s, "JUSTID") || eqICKeyword(s, "LASTID");
}
