module dreads.det;

// Determinism context: the single source of "now" for command execution.
//
// A Raft state machine may only be mutated by applying COMMITTED log entries,
// and every replica must apply them identically. Wall-clock time is the main
// non-determinism in the command set (EXPIRE, XADD *, SETEX, ...). So the
// leader freezes the clock once per command and the log entry carries it as
// provenance/truth (e.g. EXPIRE resolves to PEXPIREAT <abs> -- 100); on apply,
// every replica runs against that frozen clock, not its own.
//
// Standalone (no replication) pays nothing: dispatch sets gClock = wall time
// once per command — actually cheaper than the old scattered nowMs() calls,
// which now collapse to one read per command.

import dreads.stream : wallNow = nowMs;

/// Frozen clock for the command in flight (ms). 0 means "use the wall clock".
/// dispatch sets this at entry; the replicator injects the logged value when
/// applying a committed entry. Thread-local: dispatch and the raft apply loop
/// run on the same (event-loop) thread, and a per-thread clock is exactly what
/// the shared-nothing per-shard future wants.
public ulong gClock;

/// The command-consistent now: the frozen clock, or the wall clock as a
/// fallback for code paths reached outside dispatch (tests, timers).
public ulong now() @nogc nothrow
{
    return gClock != 0 ? gClock : wallNow();
}

/// Called by dispatch at entry. applyTime != 0 injects a replicated entry's
/// logged clock; 0 freezes the current wall time for a fresh command.
public void freezeClock(ulong applyTime) @nogc nothrow
{
    gClock = applyTime != 0 ? applyTime : wallNow();
}

// REGRESSION (background active-expire was dead): gClock is frozen per command
// and NOT reset afterwards, so a background timer that reads detNow() would see
// the last command's stale clock and never see a key as due while idle. The fix
// is the active-expire timer calling freezeClock(0) to re-pin wall time each
// cycle. This asserts that mechanism: a stale frozen clock is replaced by real
// wall time, so `now()` advances in a background context.
@nogc nothrow unittest
{
    immutable saved = gClock;
    scope (exit)
        gClock = saved;
    gClock = 1000; // a command froze the clock at t = 1000 ms
    assert(now() == 1000); // during the command, now() == the frozen clock
    freezeClock(0); // a background timer re-pins the wall clock (the fix)
    assert(gClock != 1000); // the stale value is gone...
    assert(gClock >= 1_600_000_000_000UL); // ...replaced by real epoch-ms wall time
    assert(now() == gClock); // and now() tracks it, so idle deadlines come due
}
