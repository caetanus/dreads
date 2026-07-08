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
