module dreads.pubsub;

// Pub/Sub registry. Subscribers are opaque (ctx, sink) pairs so the registry
// is testable without sockets: in production the sink locks the connection's
// write mutex and writes to the TCPConnection; in tests it appends to a
// buffer. No GC allocations here — subscriber lists are malloc'd arrays and
// messages are staged in a scratch ByteBuffer.

import core.stdc.stdlib : malloc, realloc, cfree = free;

import dreads.commands : globMatch;
import dreads.dict : Dict, Unit;
import dreads.mem : ByteBuffer;
import dreads.resp : repArrayHeader, repBulk;

/// One connected client from the registry's point of view.
public struct Subscriber
{
    void* ctx;
    void function(void* ctx, scope const(ubyte)[] bytes) nothrow sink;
    Dict!Unit channels;
    Dict!Unit patterns;

    @property size_t subCount() const @nogc nothrow
    {
        return channels.length + patterns.length;
    }

    void free() @nogc nothrow
    {
        channels.free();
        patterns.free();
    }
}

/// Growable malloc'd array of subscriber pointers.
private struct SubList
{
    Subscriber** items;
    size_t len;
    size_t cap;

    void add(Subscriber* s) @nogc nothrow
    {
        if (len == cap)
        {
            cap = cap ? cap * 2 : 4;
            items = cast(Subscriber**) realloc(items, cap * (Subscriber*).sizeof);
            assert(items !is null, "out of memory");
        }
        items[len++] = s;
    }

    /// Returns true when the subscriber was present.
    bool remove(Subscriber* s) @nogc nothrow
    {
        foreach (i; 0 .. len)
        {
            if (items[i] is s)
            {
                items[i] = items[len - 1];
                len--;
                return true;
            }
        }
        return false;
    }

    void free() @nogc nothrow
    {
        if (items !is null)
            cfree(items);
        items = null;
        len = cap = 0;
    }
}

public struct PubSub
{
    private Dict!SubList channels; // channel -> subscribers
    private SubList patternSubs; // every subscriber holding at least one pattern
    private ByteBuffer scratch; // message staging, reused across publishes

    /// True when the channel subscription is new for this subscriber.
    bool subscribe(Subscriber* s, scope const(char)[] channel) @nogc nothrow
    {
        if (!s.channels.set(channel, Unit()))
            return false;
        auto list = channels.get(channel);
        if (list is null)
        {
            channels.set(channel, SubList());
            list = channels.get(channel);
        }
        list.add(s);
        return true;
    }

    bool unsubscribe(Subscriber* s, scope const(char)[] channel) @nogc nothrow
    {
        if (!s.channels.del(channel))
            return false;
        auto list = channels.get(channel);
        if (list !is null)
        {
            list.remove(s);
            if (list.len == 0)
                channels.del(channel);
        }
        return true;
    }

    bool psubscribe(Subscriber* s, scope const(char)[] pattern) @nogc nothrow
    {
        if (!s.patterns.set(pattern, Unit()))
            return false;
        if (s.patterns.length == 1)
            patternSubs.add(s);
        return true;
    }

    bool punsubscribe(Subscriber* s, scope const(char)[] pattern) @nogc nothrow
    {
        if (!s.patterns.del(pattern))
            return false;
        if (s.patterns.length == 0)
            patternSubs.remove(s);
        return true;
    }

    /// Removes every subscription (connection teardown).
    void dropAll(Subscriber* s) @nogc nothrow
    {
        foreach (ch, ref u; s.channels)
        {
            auto list = channels.get(ch);
            if (list !is null)
            {
                list.remove(s);
                if (list.len == 0)
                    channels.del(ch);
            }
        }
        s.channels.clear();
        if (s.patterns.length > 0)
            patternSubs.remove(s);
        s.patterns.clear();
    }

    /// Delivers to channel and pattern subscribers; returns the receiver count.
    /// verb is "message" for regular pub/sub, "smessage" for shard channels.
    long publish(scope const(char)[] channel, scope const(char)[] payload,
            scope const(char)[] verb = "message") nothrow
    {
        long receivers = 0;
        auto list = channels.get(channel);
        if (list !is null && list.len > 0)
        {
            scratch.clear();
            repArrayHeader(scratch, 3);
            repBulk(scratch, verb);
            repBulk(scratch, channel);
            repBulk(scratch, payload);
            foreach (i; 0 .. list.len)
            {
                auto s = list.items[i];
                s.sink(s.ctx, scratch.data);
                receivers++;
            }
        }
        foreach (i; 0 .. patternSubs.len)
        {
            auto s = patternSubs.items[i];
            // index iteration: the loop body calls the (non-@nogc) sink, so it
            // cannot be an opApply delegate
            foreach (pi; 0 .. s.patterns.capacity)
            {
                if (!s.patterns.slotLive(pi))
                    continue;
                auto pat = s.patterns.keyAt(pi);
                if (!globMatch(pat, channel))
                    continue;
                scratch.clear();
                repArrayHeader(scratch, 4);
                repBulk(scratch, "pmessage");
                repBulk(scratch, pat);
                repBulk(scratch, channel);
                repBulk(scratch, payload);
                s.sink(s.ctx, scratch.data);
                receivers++;
            }
        }
        return receivers;
    }

    /// PUBSUB CHANNELS [pattern]
    int eachChannel(scope const(char)[] pattern,
            scope int delegate(const(char)[] channel, size_t nsubs) @nogc nothrow dg) @nogc nothrow
    {
        foreach (ch, ref list; channels)
        {
            if (pattern.length && !globMatch(pattern, ch))
                continue;
            auto r = dg(ch, list.len);
            if (r)
                return r;
        }
        return 0;
    }

    size_t channelSubCount(scope const(char)[] channel) @nogc nothrow
    {
        auto list = channels.get(channel);
        return list is null ? 0 : list.len;
    }

    /// Total pattern subscriptions (Redis counts unique patterns; see DRIFT).
    size_t patternCount() const @nogc nothrow
    {
        size_t n = 0;
        foreach (i; 0 .. patternSubs.len)
            n += patternSubs.items[i].patterns.length;
        return n;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    private struct FakeClient
    {
        Subscriber sub;
        ByteBuffer received;

        static void sinkFn(void* ctx, scope const(ubyte)[] bytes) nothrow
        {
            (cast(FakeClient*) ctx).received.append(bytes);
        }

        void init_()
        {
            sub.ctx = &this;
            sub.sink = &sinkFn;
        }

        string got()
        {
            return (cast(string) received.data).idup;
        }
    }
}

unittest // subscribe/publish/unsubscribe flow
{
    PubSub ps;
    FakeClient a, b;
    a.init_();
    b.init_();
    scope (exit)
    {
        a.sub.free();
        b.sub.free();
    }

    assert(ps.subscribe(&a.sub, "news"));
    assert(!ps.subscribe(&a.sub, "news")); // duplicate
    assert(ps.subscribe(&b.sub, "news"));
    assert(a.sub.subCount == 1);
    assert(ps.channelSubCount("news") == 2);

    assert(ps.publish("news", "hello") == 2);
    enum msg = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    assert(a.got == msg && b.got == msg);

    assert(ps.publish("empty", "x") == 0);

    assert(ps.unsubscribe(&a.sub, "news"));
    assert(!ps.unsubscribe(&a.sub, "news"));
    assert(ps.channelSubCount("news") == 1);
    a.received.clear();
    assert(ps.publish("news", "again") == 1);
    assert(a.got == "");
}

unittest // pattern subscriptions
{
    PubSub ps;
    FakeClient p;
    p.init_();
    scope (exit)
        p.sub.free();

    assert(ps.psubscribe(&p.sub, "user:*"));
    assert(!ps.psubscribe(&p.sub, "user:*"));
    assert(ps.publish("user:42", "hi") == 1);
    assert(p.got == "*4\r\n$8\r\npmessage\r\n$6\r\nuser:*\r\n$7\r\nuser:42\r\n$2\r\nhi\r\n");
    assert(ps.publish("other", "no") == 0);

    // both channel and pattern deliveries count
    FakeClient c;
    c.init_();
    scope (exit)
        c.sub.free();
    ps.subscribe(&c.sub, "user:42");
    p.received.clear();
    assert(ps.publish("user:42", "x") == 2);

    assert(ps.punsubscribe(&p.sub, "user:*"));
    assert(ps.publish("user:42", "y") == 1); // only the channel subscriber now
}

unittest // dropAll cleans every registration
{
    PubSub ps;
    FakeClient a;
    a.init_();
    scope (exit)
        a.sub.free();
    ps.subscribe(&a.sub, "c1");
    ps.subscribe(&a.sub, "c2");
    ps.psubscribe(&a.sub, "p*");
    assert(a.sub.subCount == 3);
    ps.dropAll(&a.sub);
    assert(a.sub.subCount == 0);
    assert(ps.publish("c1", "x") == 0);
    assert(ps.publish("pq", "x") == 0);
    size_t n;
    ps.eachChannel(null, (ch, cnt) { n++; return 0; });
    assert(n == 0);
}
