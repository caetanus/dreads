module dreads.stats;

// INFO # Errorstats + total_error_replies. (Per-command commandstats lives in
// dreads.server as gCmdStats/statCall/statRejected.) Counted at the top of the
// command pipeline on the EXECUTING thread (thread-local tallies; shard-local
// INFO is the v1 convention) — a script's redis.call round-trips to the main
// thread, so top-level commands and script sub-calls flow through one choke
// point there.
//
// Model mirrors Valkey (afterErrorReply guarded by a prev-error-count): each REAL
// leaf error bumps total_error_replies + errorstat_<code> exactly once; a script
// re-reporting an inner command's error does NOT re-count it (total already grew
// while the sub-call ran) — that only takes a failed_calls in commandstats.

import dreads.dict : Dict;
import dreads.mem : ByteBuffer;
import dreads.aclcat : gCmdCats;

// INFO # Commandstats. `rejected` = refused before execution (ACL / deny-OOM);
// `failed` = executed but returned an error. Indexed by aclCmdIndex(name) so the
// update is an O(1) array bump on the main thread (no hash on the hot path).
public struct CmdStat
{
    ulong calls, usec, rejected, failed;
}

// THREAD-LOCAL (share-nothing rule): each shard counts the commands IT ran;
// INFO commandstats reports the serving shard's view (the established v1
// convention — see obj.d gConnectedClients; cross-shard aggregation = 2b).
// As __gshared these were lost-update races from every listener thread.
public CmdStat[gCmdCats.length] gCmdStats;

public void statRejected(int idx) @nogc nothrow
{
    if (idx >= 0 && idx < cast(int) gCmdStats.length)
        gCmdStats[idx].rejected++;
}

public void statCall(int idx, bool failed) @nogc nothrow
{
    if (idx >= 0 && idx < cast(int) gCmdStats.length)
    {
        gCmdStats[idx].calls++;
        if (failed)
            gCmdStats[idx].failed++;
    }
}

public void resetCmdStats() @nogc nothrow
{
    foreach (ref s; gCmdStats)
        s = CmdStat.init;
}

// THREAD-LOCAL: same shard-local v1 convention as gCmdStats — and as __gshared
// the Dict was a cross-thread rehash double-free (the conn-registry crash class).
public Dict!ulong gErrorStats; // error code (OOM/ERR/WRONGTYPE/…) -> count
public ulong gTotalErrorReplies;

// Count one leaf error reply: bump total + errorstat_<code>. `reply` is the raw
// RESP error (with or without the leading '-'); the code is the first token,
// capped at 32 chars like Valkey, defaulting to ERR.
public void statErrorReply(scope const(char)[] reply) nothrow @nogc
{
    if (reply.length == 0)
        return;
    immutable size_t s = reply[0] == '-' ? 1 : 0;
    size_t e = s;
    immutable cap = s + 32 < reply.length ? s + 32 : reply.length;
    while (e < cap && reply[e] != ' ' && reply[e] != '\r' && reply[e] != '\n')
        e++;
    auto code = e > s ? reply[s .. e] : "ERR";
    gTotalErrorReplies++;
    if (auto p = gErrorStats.get(code))
    {
        (*p)++;
        return;
    }
    gErrorStats.set(code, 1);
}

public void resetErrorStats() nothrow @nogc
{
    gErrorStats.clear();
    gTotalErrorReplies = 0;
}

public void appendErrorstats(ref ByteBuffer o) nothrow @nogc
{
    import core.stdc.stdio : snprintf;

    o.append("# Errorstats\r\n");
    char[128] b = void;
    foreach (i; 0 .. gErrorStats.capacity)
    {
        if (!gErrorStats.slotLive(i))
            continue;
        auto code = gErrorStats.keyAt(i);
        immutable n = snprintf(b.ptr, b.length, "errorstat_%.*s:count=%llu\r\n",
            cast(int) code.length, code.ptr, *gErrorStats.valAt(i));
        o.append(b[0 .. n]);
    }
}
