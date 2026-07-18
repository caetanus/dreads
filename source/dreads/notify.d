module dreads.notify;

// Redis keyspace notifications. A keyspace-mutating command fires an event that,
// gated by the `notify-keyspace-events` flags, publishes to:
//   __keyspace@<db>__:<key>    with message = <event>   (K flag)
//   __keyevent@<db>__:<event>  with message = <key>     (E flag)
// The <db> is the index of the database the command touched (gNotifyDb, set by
// the dispatch and the active-expire/eviction cycles). The command layer (commands.d)
// calls notifyKeyspaceEvent without depending on the server/pubsub module: the
// server installs `gNotifyPublish` pointing at gPubSub.publish.

import dreads.mem : ByteBuffer;

/// Event-class bits. K/E select WHERE to publish; the rest are command classes.
enum NClass : uint
{
    keyspace = 1 << 0, // K
    keyevent = 1 << 1, // E
    generic = 1 << 2, // g
    str = 1 << 3, // $
    list = 1 << 4, // l
    set = 1 << 5, // s
    hash = 1 << 6, // h
    zset = 1 << 7, // z
    expired = 1 << 8, // x
    evicted = 1 << 9, // e
    stream = 1 << 10, // t
    keymiss = 1 << 11, // m
    newkey = 1 << 12, // n
    modevt = 1 << 13, // d
    // "A" alias = "g$lshzxet[d]" — every class except keymiss/newkey.
    all = generic | str | list | set | hash | zset | expired | evicted | stream | modevt,
}

__gshared uint gNotifyFlags = 0;
__gshared void delegate(scope const(char)[] chan, scope const(char)[] msg) nothrow gNotifyPublish;
// PUBLISH/SPUBLISH from the data plane (a redis.call inside a script reaches the
// pub/sub layer through this, since dispatch can't touch the server module). The
// server installs it pointing at gPubSub/gShardPubSub.publish; returns the number
// of clients the message reached. `shard` selects the sharded channel space.
__gshared long delegate(scope const(char)[] chan, scope const(char)[] msg, bool shard) nothrow gPublishHook;
// Events are QUEUED here (length-prefixed chan,msg pairs) by notifyKeyspaceEvent,
// which runs inside the @nogc command dispatch and so cannot publish directly.
// The server drains this via flushPendingNotify() after the command, on the
// non-@nogc side. Single event-loop thread, so a plain global buffer is safe.
private __gshared ByteBuffer pending;

private void appU32(ref ByteBuffer b, uint v) @nogc nothrow
{
    b.appendByte(cast(char)(v & 0xFF));
    b.appendByte(cast(char)((v >> 8) & 0xFF));
    b.appendByte(cast(char)((v >> 16) & 0xFF));
    b.appendByte(cast(char)((v >> 24) & 0xFF));
}

// Queue one (chanA ~ chanB, msg) pair.
private void queuePair(scope const(char)[] chanA, scope const(char)[] chanB,
        scope const(char)[] msg) @nogc nothrow
{
    appU32(pending, cast(uint)(chanA.length + chanB.length));
    pending.append(chanA);
    pending.append(chanB);
    appU32(pending, cast(uint) msg.length);
    pending.append(msg);
}

/// Render a flag bitmask back to its CANONICAL string (Redis's
/// keyspaceEventsFlagsToString order), so CONFIG GET round-trips normalized:
/// class flags (or `A` when all present) g$lshzxetd, then m, n, then K, E.
/// `A` collapses the class group only — m/n stay explicit (they're not in A).
const(char)[] notifyFlagsToString(uint flags, return scope char[] buf) @nogc nothrow
{
    size_t n = 0;
    if ((flags & NClass.all) == NClass.all)
        buf[n++] = 'A';
    else
    {
        if (flags & NClass.generic) buf[n++] = 'g';
        if (flags & NClass.str) buf[n++] = '$';
        if (flags & NClass.list) buf[n++] = 'l';
        if (flags & NClass.set) buf[n++] = 's';
        if (flags & NClass.hash) buf[n++] = 'h';
        if (flags & NClass.zset) buf[n++] = 'z';
        if (flags & NClass.expired) buf[n++] = 'x';
        if (flags & NClass.evicted) buf[n++] = 'e';
        if (flags & NClass.stream) buf[n++] = 't';
        if (flags & NClass.modevt) buf[n++] = 'd';
    }
    if (flags & NClass.keymiss) buf[n++] = 'm';
    if (flags & NClass.newkey) buf[n++] = 'n';
    if (flags & NClass.keyspace) buf[n++] = 'K';
    if (flags & NClass.keyevent) buf[n++] = 'E';
    return buf[0 .. n];
}

/// Parse the flag string ("KEA", "Kxge", ...) into a bitmask. false on a bad char.
bool parseNotifyFlags(scope const(char)[] s, out uint flags) @nogc nothrow
{
    foreach (c; s)
    {
        switch (c)
        {
        case 'K': flags |= NClass.keyspace; break;
        case 'E': flags |= NClass.keyevent; break;
        case 'g': flags |= NClass.generic; break;
        case '$': flags |= NClass.str; break;
        case 'l': flags |= NClass.list; break;
        case 's': flags |= NClass.set; break;
        case 'h': flags |= NClass.hash; break;
        case 'z': flags |= NClass.zset; break;
        case 'x': flags |= NClass.expired; break;
        case 'e': flags |= NClass.evicted; break;
        case 't': flags |= NClass.stream; break;
        case 'm': flags |= NClass.keymiss; break;
        case 'n': flags |= NClass.newkey; break;
        case 'd': flags |= NClass.modevt; break;
        case 'A': flags |= NClass.all; break;
        default: return false;
        }
    }
    return true;
}

/// Queue an event of `klass` (published after the command). No-op unless the
/// class is enabled and at least one of K/E is set. @nogc: safe to call from the
/// command dispatch. Cheap in the common (disabled) case: one bit test.
/// The db index the notification channels are shaped for — set by the command
/// dispatch (and the active-expire/eviction loops) to the keyspace being touched,
/// so `__keyspace@<db>__` / `__keyevent@<db>__` name the RIGHT database, not 0.
public __gshared int gNotifyDb;

void notifyKeyspaceEvent(uint klass, scope const(char)[] event, scope const(char)[] key) @nogc nothrow
{
    // Delegate to the db-aware formatter with the current command's db — the
    // channel is `__keyspace@<gNotifyDb>__`, not a hardcoded @0.
    notifyKeyspaceEventDb(gNotifyDb, klass, event, key);
}

/// Like notifyKeyspaceEvent but targets a SPECIFIC db's channels — needed by the
/// cross-db commands (MOVE fires `move_from` on the source db and `move_to` on the
/// destination db). Formats `__keyspace@<db>__:` / `__keyevent@<db>__:` @nogc.
void notifyKeyspaceEventDb(int db, uint klass, scope const(char)[] event,
        scope const(char)[] key) @nogc nothrow
{
    import core.stdc.stdio : snprintf;

    immutable f = gNotifyFlags;
    if (!(f & klass))
        return;
    char[32] ksbuf = void, kebuf = void;
    immutable kn = snprintf(ksbuf.ptr, ksbuf.length, "__keyspace@%d__:", db);
    immutable en = snprintf(kebuf.ptr, kebuf.length, "__keyevent@%d__:", db);
    // snprintf returns the would-be length; guard against a truncating (too-large)
    // db so the slice can never run past the buffer.
    if (kn <= 0 || kn > ksbuf.length || en <= 0 || en > kebuf.length)
        return;
    // `new` (and `keymiss`) are keyevent-only in Redis — there is no
    // __keyspace__:new channel — so they never take the keyspace branch.
    immutable eventOnly = (klass & (NClass.newkey | NClass.keymiss)) != 0;
    if ((f & NClass.keyspace) && !eventOnly)
        queuePair(ksbuf[0 .. kn], key, event);
    if (f & NClass.keyevent)
        queuePair(kebuf[0 .. en], event, key);
}

/// Publish and clear everything queued since the last flush. Called by the server
/// after each command, on the non-@nogc side (gNotifyPublish -> gPubSub.publish).
void flushPendingNotify() nothrow
{
    if (pending.length == 0)
        return;
    if (gNotifyPublish !is null)
    {
        auto d = pending.data;
        size_t p = 0;
        while (p + 4 <= d.length)
        {
            immutable cl = d[p] | (d[p + 1] << 8) | (d[p + 2] << 16) | (d[p + 3] << 24);
            p += 4;
            if (p + cl + 4 > d.length) // truncated/corrupt frame: stop, don't over-slice
                break;
            auto chan = cast(const(char)[]) d[p .. p + cl];
            p += cl;
            immutable ml = d[p] | (d[p + 1] << 8) | (d[p + 2] << 16) | (d[p + 3] << 24);
            p += 4;
            if (p + ml > d.length)
                break;
            auto msg = cast(const(char)[]) d[p .. p + ml];
            p += ml;
            gNotifyPublish(chan, msg);
        }
    }
    pending.clear();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;
}

unittest // flag parsing
{
    uint f;
    assert(parseNotifyFlags("", f) && f == 0);
    assert(parseNotifyFlags("KEA", f));
    assert((f & NClass.keyspace) && (f & NClass.keyevent) && (f & NClass.str) && (f & NClass.generic));
    assert(!(f & NClass.keymiss)); // A excludes m/n
    f = 0;
    assert(parseNotifyFlags("Kx", f) && (f & NClass.keyspace) && (f & NClass.expired) && !(f & NClass.keyevent));
    assert(!parseNotifyFlags("KZ", f)); // 'Z' is not a valid flag
}

unittest // event routing to keyspace / keyevent channels
{
    static struct Cap
    {
        __gshared const(char)[] chan, msg;
        __gshared int n;
    }
    gNotifyPublish = (scope const(char)[] c, scope const(char)[] m) nothrow{
        Cap.chan = c.idup;
        Cap.msg = m.idup;
        Cap.n++;
    };
    scope (exit)
    {
        gNotifyPublish = null;
        gNotifyFlags = 0;
    }

    // class disabled -> nothing
    gNotifyFlags = NClass.keyspace | NClass.keyevent; // no class bits
    Cap.n = 0;
    notifyKeyspaceEvent(NClass.str, "set", "foo");
    flushPendingNotify();
    assert(Cap.n == 0);

    // K only
    gNotifyFlags = NClass.keyspace | NClass.str;
    Cap.n = 0;
    notifyKeyspaceEvent(NClass.str, "set", "foo");
    flushPendingNotify();
    assert(Cap.n == 1 && Cap.chan == "__keyspace@0__:foo" && Cap.msg == "set");

    // E only
    gNotifyFlags = NClass.keyevent | NClass.generic;
    Cap.n = 0;
    notifyKeyspaceEvent(NClass.generic, "del", "bar");
    flushPendingNotify();
    assert(Cap.n == 1 && Cap.chan == "__keyevent@0__:del" && Cap.msg == "bar");

    // both K and E -> two publishes
    gNotifyFlags = NClass.keyspace | NClass.keyevent | NClass.generic;
    Cap.n = 0;
    notifyKeyspaceEvent(NClass.generic, "expire", "k");
    flushPendingNotify();
    assert(Cap.n == 2);
}
