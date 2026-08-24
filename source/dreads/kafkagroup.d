module dreads.kafkagroup;

// Kafka consumer-group coordinator (classic protocol, KIP-848 out of scope).
//
// OWNERSHIP: a group's state lives in TLS on the shard that owns
// keyToSlot("kafka.cg.<group>") — the same slot as its committed-offsets hash,
// so membership and offsets are co-owned. Every state transition executes in
// the owner's drain (which never yields), so each op is atomic by
// construction: no locks, no broadcast.
//
// WAIT MODEL: the coordinator never parks. Ops that must "hold" a response
// (JoinGroup barrier, follower SyncGroup) answer KG_WAIT; the CLIENT fiber
// (on its connection's shard) polls with a short sleep, bounded by the
// rebalance timeout. Per-connection head-of-line blocking during a join is
// exactly what a real broker does (in-order processing per connection).
//
// Transport: server.d routes kgroup requests via ShardMsg.kafkaGroup
// ([u64 pend][request]) and calls kgroupApply() in the owner's drain; the
// same-shard case calls it directly. See KAFKA-GROUPS-PLAN.md.

import core.time : MonoTime;

import dreads.mem : ByteBuffer;

// Kafka error codes surfaced by the FSM (wire codes, except KG_WAIT).
public enum short KG_NONE = 0;
public enum short KG_ILLEGAL_GENERATION = 22;
public enum short KG_INCONSISTENT_PROTOCOL = 23;
public enum short KG_UNKNOWN_MEMBER = 25;
public enum short KG_REBALANCE_IN_PROGRESS = 27;
public enum short KG_MEMBER_ID_REQUIRED = 79;
/// Internal sentinel: not a wire code — the caller sleeps and re-polls.
public enum short KG_WAIT = -100;

// Request opcodes ([u8 op][u16 gLen][group][...]; all ints big-endian).
public enum ubyte KGOP_JOIN = 1; // fresh JoinGroup (may trigger a rebalance)
public enum ubyte KGOP_JOIN_POLL = 2; // barrier poll (never triggers)
public enum ubyte KGOP_SYNC = 3; // SyncGroup; leader carries assignments
public enum ubyte KGOP_HEARTBEAT = 4;
public enum ubyte KGOP_LEAVE = 5;
public enum ubyte KGOP_DESCRIBE = 6;
public enum ubyte KGOP_COMMIT_CHECK = 7; // OffsetCommit generation fencing

private enum ubyte ST_EMPTY = 0, ST_PREPARING = 1, ST_COMPLETING = 2, ST_STABLE = 3;

private enum long REBALANCE_CAP_MS = 60_000; // barrier deadline ceiling
private enum size_t KG_MAX_MEMBERS = 512; // sanity cap per group
private enum size_t KG_MAX_GROUPS = 4096; // sanity cap per shard

private struct KgMember
{
    string gii; // group.instance.id; "" = dynamic member
    string protoType;
    string[] protoNames; // supported protocols, preference order
    immutable(ubyte)[][] protoMetas; // metadata per protocol (same order)
    int sessMs = 45_000;
    int rebMs = 60_000;
    long lastMs; // MonoTime ms of the member's last op (heartbeat surrogate)
    bool joinedRound; // re-joined during the current rebalance round
    immutable(ubyte)[] assignment;
    bool hasAssignment;
}

private struct KgGroup
{
    ubyte state = ST_EMPTY;
    int generation = 0;
    string leader;
    string protoName;
    string protoType;
    long deadlineMs; // PreparingRebalance barrier deadline
    string[] order; // member ids in join order (leader pick + stable listing)
    KgMember[string] members;
}

private KgGroup*[string] tGroups; // TLS: groups owned by THIS shard

/// Monotonic milliseconds (never the per-command frozen wall clock).
public long kgNowMs() @nogc nothrow @trusted
{
    auto t = MonoTime.currTime;
    return t.ticks / (MonoTime.ticksPerSecond / 1000);
}

// --- little wire helpers over the op payloads --------------------------------

private struct KgRd
{
    const(ubyte)[] p;
    size_t i;
    bool ok = true;

    ubyte u8() nothrow @nogc
    {
        if (i + 1 > p.length)
        {
            ok = false;
            return 0;
        }
        return p[i++];
    }

    int i32() nothrow @nogc
    {
        if (i + 4 > p.length)
        {
            ok = false;
            return 0;
        }
        immutable v = (cast(int) p[i] << 24) | (cast(int) p[i + 1] << 16)
            | (cast(int) p[i + 2] << 8) | p[i + 3];
        i += 4;
        return v;
    }

    const(char)[] str16() nothrow @nogc
    {
        if (i + 2 > p.length)
        {
            ok = false;
            return null;
        }
        immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
        i += 2;
        if (i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + n];
        i += n;
        return s;
    }

    const(ubyte)[] bytes32() nothrow @nogc
    {
        immutable n = cast(uint) i32();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto b = p[i .. i + n];
        i += n;
        return b;
    }
}

private void wU8(ref ByteBuffer o, ubyte v) nothrow @nogc
{
    o.appendByte(cast(char) v);
}

private void wI16(ref ByteBuffer o, short v) nothrow @nogc
{
    o.appendByte(cast(char)(cast(ushort) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void wI32(ref ByteBuffer o, int v) nothrow @nogc
{
    o.appendByte(cast(char)(cast(uint) v >> 24));
    o.appendByte(cast(char)(cast(uint) v >> 16));
    o.appendByte(cast(char)(cast(uint) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void wStr16(ref ByteBuffer o, scope const(char)[] s) nothrow
{
    immutable n = s.length > 0xFFFF ? 0xFFFF : s.length;
    wI16(o, cast(short) n);
    o.append(s[0 .. n]);
}

private void wBytes32(ref ByteBuffer o, scope const(ubyte)[] b) nothrow
{
    wI32(o, cast(int) b.length);
    o.append(cast(const(char)[]) b);
}

// --- FSM ---------------------------------------------------------------------

private void enterPreparing(KgGroup* g, long now) nothrow
{
    g.state = ST_PREPARING;
    long maxReb = 5000;
    foreach (ref m; g.members)
    {
        if (m.rebMs > maxReb)
            maxReb = m.rebMs;
        m.joinedRound = false;
    }
    if (maxReb > REBALANCE_CAP_MS)
        maxReb = REBALANCE_CAP_MS;
    g.deadlineMs = now + maxReb;
}

/// Close the join barrier: bump generation, elect the leader, select the
/// protocol every member supports. Returns false when no common protocol
/// exists (the group resets to Empty; joiners see KG_INCONSISTENT_PROTOCOL).
private bool closeBarrier(KgGroup* g) nothrow
{
    // drop members that missed the round (deadline path); joined ones stay
    string[] keep;
    foreach (id; g.order)
        if (auto m = id in g.members)
        {
            if (m.joinedRound)
                keep ~= id;
            else
                g.members.remove(id);
        }
    g.order = keep;
    if (g.order.length == 0)
    {
        g.state = ST_EMPTY;
        return true;
    }
    // leader: previous leader when it re-joined, else first in join order
    bool leaderAlive = false;
    foreach (id; g.order)
        if (id == g.leader)
        {
            leaderAlive = true;
            break;
        }
    if (!leaderAlive)
        g.leader = g.order[0];
    // protocol: leader's preference order, first supported by all members
    auto lead = g.leader in g.members;
    g.protoName = null;
    if (lead !is null)
        outer: foreach (cand; lead.protoNames)
        {
            foreach (id; g.order)
            {
                auto m = id in g.members;
                bool has = false;
                foreach (pn; m.protoNames)
                    if (pn == cand)
                    {
                        has = true;
                        break;
                    }
                if (!has)
                    continue outer;
            }
            g.protoName = cand;
            break;
        }
    if (g.protoName is null)
    {
        // no common protocol: reset — every parked joiner answers 23
        g.state = ST_EMPTY;
        g.generation++;
        return false;
    }
    g.protoType = lead.protoType;
    g.generation++;
    g.state = ST_COMPLETING;
    foreach (ref m; g.members)
    {
        m.hasAssignment = false;
        m.assignment = null;
    }
    return true;
}

/// The member's selected-protocol metadata (for the leader's member list).
private immutable(ubyte)[] memberMeta(const KgMember* m, scope const(char)[] proto) nothrow
{
    foreach (k, pn; m.protoNames)
        if (pn == proto && k < m.protoMetas.length)
            return m.protoMetas[k];
    return m.protoMetas.length ? m.protoMetas[0] : null;
}

/// Emit the join-ok payload for `mid`:
/// [i16 0][str mid][i32 gen][u8 isLeader][str proto][str protoType][str leader]
/// [i32 nmembers]{[str mid][u8 giiNull][str gii][bytes32 meta]}* (leader only)
private void emitJoinOk(KgGroup* g, scope const(char)[] mid, ref ByteBuffer o) nothrow
{
    immutable isLeader = mid == g.leader;
    wI16(o, KG_NONE);
    wStr16(o, mid); // the id this member ended up with (assigned/reclaimed)
    wI32(o, g.generation);
    wU8(o, isLeader ? 1 : 0);
    wStr16(o, g.protoName);
    wStr16(o, g.protoType);
    wStr16(o, g.leader);
    if (!isLeader)
    {
        wI32(o, 0);
        return;
    }
    wI32(o, cast(int) g.order.length);
    foreach (id; g.order)
    {
        auto m = id in g.members;
        wStr16(o, id);
        wU8(o, m.gii.length ? 0 : 1);
        wStr16(o, m.gii);
        wBytes32(o, memberMeta(m, g.protoName));
    }
}

private shared int gKgMemberCtr; // broker-assigned member-id counter

/// Execute ONE group op atomically on the owner shard. `req` is the op
/// payload (pend already stripped); the reply layout is op-specific and
/// always starts with [i16 err] (KG_WAIT = poll again).
public void kgroupApply(scope const(ubyte)[] req, ref ByteBuffer o) nothrow @trusted
{
    o.clear();
    KgRd r = KgRd(req);
    immutable op = r.u8();
    auto groupName = r.str16();
    if (!r.ok || groupName.length == 0)
    {
        wI16(o, KG_UNKNOWN_MEMBER);
        return;
    }
    immutable now = kgNowMs();
    auto gp = groupName in tGroups;
    KgGroup* g = gp !is null ? *gp : null;

    switch (op)
    {
    default:
        wI16(o, KG_UNKNOWN_MEMBER); // unknown op: safe terminal error
        return;

    case KGOP_JOIN:
        {
            auto mid = r.str16();
            immutable giiNull = r.u8() != 0;
            auto gii = r.str16();
            immutable sessMs = r.i32();
            immutable rebMs = r.i32();
            immutable v4plus = r.u8() != 0;
            auto ptype = r.str16();
            immutable np = r.i32();
            string[] pnames;
            immutable(ubyte)[][] pmetas;
            foreach (_; 0 .. (np < 0 || np > 64 ? 0 : np))
            {
                if (!r.ok)
                    break;
                auto pn = r.str16();
                auto pm = r.bytes32();
                if (r.ok)
                {
                    pnames ~= pn.idup;
                    pmetas ~= cast(immutable(ubyte)[]) pm.idup;
                }
            }
            if (!r.ok || pnames.length == 0)
            {
                wI16(o, KG_INCONSISTENT_PROTOCOL);
                return;
            }
            if (g is null)
            {
                if (tGroups.length >= KG_MAX_GROUPS)
                {
                    wI16(o, KG_REBALANCE_IN_PROGRESS); // safe retryable
                    return;
                }
                g = new KgGroup;
                tGroups[groupName.idup] = g;
            }
            // static membership: a known group.instance.id reclaims its id
            string useMid = null;
            if (!giiNull && gii.length)
                foreach (id; g.order)
                    if (auto m = id in g.members)
                        if (m.gii == gii)
                        {
                            useMid = id;
                            break;
                        }
            if (useMid is null && mid.length)
                useMid = mid.idup;
            if (useMid is null)
            {
                // broker-assigned id
                import core.atomic : atomicOp;
                import core.stdc.stdio : snprintf;

                char[64] buf = void;
                immutable n = atomicOp!"+="(gKgMemberCtr, 1);
                immutable k = snprintf(buf.ptr, buf.length, "dreads-%d-%d", n,
                        cast(int)(now & 0xFFFF));
                useMid = buf[0 .. (k > 0 ? k : 0)].idup;
                if (v4plus)
                {
                    // KIP-394 dance: register as a known-but-not-joined member
                    // (the barrier waits for its immediate re-join), answer 79.
                    if (g.members.length < KG_MAX_MEMBERS)
                    {
                        KgMember nm;
                        nm.gii = giiNull ? "" : gii.idup;
                        nm.protoType = ptype.idup;
                        nm.protoNames = pnames;
                        nm.protoMetas = pmetas;
                        nm.sessMs = sessMs > 0 ? sessMs : 45_000;
                        nm.rebMs = rebMs > 0 ? rebMs : 60_000;
                        nm.lastMs = now;
                        g.members[useMid] = nm;
                        g.order ~= useMid;
                    }
                    wI16(o, KG_MEMBER_ID_REQUIRED);
                    wStr16(o, useMid);
                    return;
                }
            }
            auto mp = useMid in g.members;
            if (mp is null)
            {
                if (g.members.length >= KG_MAX_MEMBERS)
                {
                    wI16(o, KG_REBALANCE_IN_PROGRESS);
                    return;
                }
                g.members[useMid] = KgMember.init;
                g.order ~= useMid;
                mp = useMid in g.members;
            }
            mp.gii = giiNull ? "" : gii.idup;
            mp.protoType = ptype.idup;
            mp.protoNames = pnames;
            mp.protoMetas = pmetas;
            mp.sessMs = sessMs > 0 ? sessMs : 45_000;
            mp.rebMs = rebMs > 0 ? rebMs : 60_000;
            mp.lastMs = now;
            // a fresh join re-opens the barrier unless one is already open
            if (g.state != ST_PREPARING)
                enterPreparing(g, now);
            mp.joinedRound = true;
            // barrier closes when every known member re-joined
            bool all = true;
            foreach (ref m; g.members)
                if (!m.joinedRound)
                {
                    all = false;
                    break;
                }
            if (all)
            {
                if (!closeBarrier(g))
                {
                    wI16(o, KG_INCONSISTENT_PROTOCOL);
                    return;
                }
                emitJoinOk(g, useMid, o);
                return;
            }
            wI16(o, KG_WAIT);
            wStr16(o, useMid); // caller polls with the (possibly assigned) id
            return;
        }

    case KGOP_JOIN_POLL:
        {
            auto mid = r.str16();
            if (g is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (g.state == ST_PREPARING)
            {
                if (now >= g.deadlineMs)
                {
                    if (!closeBarrier(g))
                    {
                        wI16(o, KG_INCONSISTENT_PROTOCOL);
                        return;
                    }
                    if (g.state == ST_EMPTY || (mid in g.members) is null)
                    {
                        wI16(o, KG_UNKNOWN_MEMBER);
                        return;
                    }
                    emitJoinOk(g, mid, o);
                    return;
                }
                wI16(o, KG_WAIT);
                wStr16(o, mid);
                return;
            }
            if ((g.state == ST_COMPLETING || g.state == ST_STABLE) && mp.joinedRound)
            {
                emitJoinOk(g, mid, o);
                return;
            }
            wI16(o, KG_WAIT);
            wStr16(o, mid);
            return;
        }

    case KGOP_SYNC:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            immutable nass = r.i32();
            if (g is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            if (g.state == ST_COMPLETING && mid == g.leader && nass > 0)
            {
                foreach (_; 0 .. (nass > cast(int) KG_MAX_MEMBERS ? 0 : nass))
                {
                    if (!r.ok)
                        break;
                    auto tmid = r.str16();
                    auto ab = r.bytes32();
                    if (!r.ok)
                        break;
                    if (auto tm = tmid in g.members)
                    {
                        tm.assignment = cast(immutable(ubyte)[]) ab.idup;
                        tm.hasAssignment = true;
                    }
                }
                foreach (ref m; g.members)
                    if (!m.hasAssignment)
                    {
                        m.assignment = null;
                        m.hasAssignment = true; // empty assignment
                    }
                g.state = ST_STABLE;
            }
            if (g.state == ST_STABLE && mp.hasAssignment)
            {
                wI16(o, KG_NONE);
                wBytes32(o, mp.assignment);
                return;
            }
            wI16(o, KG_WAIT); // follower waiting for the leader's sync
            return;
        }

    case KGOP_HEARTBEAT:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            if (g is null || g.state == ST_EMPTY)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING || g.state == ST_COMPLETING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            wI16(o, KG_NONE);
            return;
        }

    case KGOP_LEAVE:
        {
            immutable nm = r.i32();
            wI16(o, KG_NONE);
            wI32(o, nm < 0 || nm > cast(int) KG_MAX_MEMBERS ? 0 : nm);
            bool removedAny = false;
            foreach (_; 0 .. (nm < 0 || nm > cast(int) KG_MAX_MEMBERS ? 0 : nm))
            {
                if (!r.ok)
                    break;
                auto mid = r.str16();
                short perr = KG_UNKNOWN_MEMBER;
                if (g !is null && (mid in g.members) !is null)
                {
                    g.members.remove(mid.idup);
                    string[] keep;
                    foreach (id; g.order)
                        if (id != mid)
                            keep ~= id;
                    g.order = keep;
                    removedAny = true;
                    perr = KG_NONE;
                }
                wI16(o, perr);
            }
            if (removedAny && g !is null)
            {
                if (g.order.length == 0)
                    g.state = ST_EMPTY;
                else
                    enterPreparing(g, now); // survivors must re-join
            }
            return;
        }

    case KGOP_DESCRIBE:
        {
            wI16(o, KG_NONE);
            if (g is null)
            {
                wU8(o, ST_EMPTY);
                wI32(o, 0);
                wStr16(o, "");
                wStr16(o, "");
                wI32(o, 0);
                return;
            }
            wU8(o, g.state);
            wI32(o, g.generation);
            wStr16(o, g.protoName);
            wStr16(o, g.protoType);
            wI32(o, cast(int) g.order.length);
            foreach (id; g.order)
            {
                auto m = id in g.members;
                wStr16(o, id);
                wU8(o, m.gii.length ? 0 : 1);
                wStr16(o, m.gii);
                wBytes32(o, memberMeta(m, g.protoName));
                wBytes32(o, m.assignment);
            }
            return;
        }

    case KGOP_COMMIT_CHECK:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            if (g is null || g.state == ST_EMPTY)
            {
                // no live group: simple consumers commit freely (legacy path)
                wI16(o, KG_NONE);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            wI16(o, KG_NONE);
            return;
        }
    }
}

/// Per-shard maintenance (piggybacks the 50ms tick): close overdue barriers,
/// evict members whose session timeout lapsed, drop empty groups.
public void kgroupSweep() nothrow @trusted
{
    if (tGroups.length == 0)
        return;
    immutable now = kgNowMs();
    string[] drop;
    foreach (name, g; tGroups)
    {
        if (g.state == ST_PREPARING && now >= g.deadlineMs)
            cast(void) closeBarrier(g);
        // session-timeout eviction (500ms slack for scheduling jitter)
        string[] dead;
        foreach (id; g.order)
            if (auto m = id in g.members)
                if (now - m.lastMs > cast(long) m.sessMs + 500)
                    dead ~= id;
        if (dead.length)
        {
            foreach (id; dead)
                g.members.remove(id);
            string[] keep;
            foreach (id; g.order)
            {
                bool gone = false;
                foreach (d; dead)
                    if (d == id)
                    {
                        gone = true;
                        break;
                    }
                if (!gone)
                    keep ~= id;
            }
            g.order = keep;
            if (g.order.length == 0)
                g.state = ST_EMPTY;
            else if (g.state == ST_STABLE || g.state == ST_COMPLETING)
                enterPreparing(g, now); // survivors re-join next heartbeat
        }
        if (g.state == ST_EMPTY && g.members.length == 0)
            drop ~= name;
    }
    foreach (name; drop)
        tGroups.remove(name);
}
