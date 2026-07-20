module chaos;

// Real-cluster chaos test for dreads' Raft consensus.
//
// Spawns N *real* dreads server processes as a Raft cluster (real TCP, real
// on-disk RaftLog), then injects worst-case chaos while clients push data:
//   - clients connect/disconnect and write all datatypes
//   - servers are killed (SIGKILL) and restarted (recover from disk)
//   - the LEADER is killed to force failover
//   - nodes are frozen (SIGSTOP) and resumed (SIGCONT)
//
// Invariants:
//   SAFETY  — after chaos heals, EVERY acknowledged write survives (a client
//             that got a success reply committed durably), and all live nodes
//             converge to the same keyspace. No acked write is ever lost.
//   LIVENESS— while a majority is alive the cluster keeps accepting writes.
//
// Standalone: Phobos + druntime only (no external deps). Build:
//   ldc2 -O2 chaos/chaos.d -of=chaos/chaos
// Run (needs a built ./bin/dreads):
//   ./chaos/chaos [seconds] [nodes]

import std.socket;
import std.stdio;
import std.conv : to;
import std.format : format;
import std.string : startsWith, strip;
import std.file : mkdirRecurse, rmdirRecurse, exists, write, remove;
import std.process : spawnProcess, Pid, kill, wait, Config, tryWait;
import std.datetime.stopwatch : StopWatch, AutoStart;
import core.thread : Thread;
import core.time : msecs, seconds, Duration, MonoTime;
import core.sys.posix.signal : SIGKILL, SIGSTOP, SIGCONT, SIGTERM;

// ---------------------------------------------------------------------------
// minimal blocking RESP client
// ---------------------------------------------------------------------------

struct Reply
{
    char kind; // '+' '-' ':' '$' '*'  ; '!' = transport error / disconnect
    string str; // status/error/bulk text (or error message for '!')
    long num; // integer reply
    bool isNil;
}

final class RespClient
{
    private Socket sock;
    private ubyte[] rbuf;
    private size_t rpos;
    bool ok;

    this(ushort port, Duration timeout = 1500.msecs)
    {
        try
        {
            sock = new TcpSocket(new InternetAddress("127.0.0.1", port));
            sock.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, timeout);
            sock.setOption(SocketOptionLevel.SOCKET, SocketOption.SNDTIMEO, timeout);
            sock.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, 1);
            ok = true;
        }
        catch (Exception)
            ok = false;
    }

    void close()
    {
        try
            if (sock !is null)
                sock.close();
        catch (Exception)
        {
        }
        ok = false;
    }

    // send a command as a RESP array of bulk strings
    Reply cmd(const(char)[][] args...)
    {
        if (!ok)
            return Reply('!', "closed");
        auto sb = format("*%d\r\n", args.length);
        foreach (a; args)
            sb ~= format("$%d\r\n%s\r\n", a.length, a);
        try
        {
            auto data = cast(const(ubyte)[]) sb;
            size_t sent = 0;
            while (sent < data.length)
            {
                auto n = sock.send(data[sent .. $]);
                if (n <= 0)
                {
                    ok = false;
                    return Reply('!', "send");
                }
                sent += n;
            }
        }
        catch (Exception)
        {
            ok = false;
            return Reply('!', "send-exc");
        }
        return readReply();
    }

    private bool fill()
    {
        ubyte[4096] tmp = void;
        try
        {
            auto n = sock.receive(tmp[]);
            if (n <= 0)
            {
                ok = false;
                return false;
            }
            rbuf ~= tmp[0 .. n];
            return true;
        }
        catch (Exception)
        {
            ok = false;
            return false;
        }
    }

    private string readLine()
    {
        for (;;)
        {
            foreach (i; rpos .. (rbuf.length == 0 ? 0 : rbuf.length - 1))
            {
                if (rbuf[i] == '\r' && rbuf[i + 1] == '\n')
                {
                    auto s = cast(string)(rbuf[rpos .. i].idup);
                    rpos = i + 2;
                    return s;
                }
            }
            if (!fill())
                return null;
        }
    }

    private Reply readReply()
    {
        auto line = readLine();
        if (!ok || line.length == 0)
            return Reply('!', "eof");
        char k = line[0];
        auto rest = line[1 .. $];
        final switch (k)
        {
        case '+':
            return Reply('+', rest);
        case '-':
            return Reply('-', rest);
        case ':':
            return Reply(':', rest, rest.to!long);
        case '$':
            auto len = rest.to!long;
            if (len < 0)
                return Reply('$', "", 0, true);
            while (rbuf.length - rpos < cast(size_t) len + 2)
                if (!fill())
                    return Reply('!', "eof-bulk");
            auto s = cast(string)(rbuf[rpos .. rpos + cast(size_t) len].idup);
            rpos += cast(size_t) len + 2;
            return Reply('$', s);
        case '*':
            // arrays: we only need the element count for our workload
            return Reply('*', rest, rest.to!long);
        }
    }

    static Reply err(string k)
    {
        return Reply('!', k);
    }
}

// ---------------------------------------------------------------------------
// cluster of real dreads processes
// ---------------------------------------------------------------------------

struct Node
{
    uint id;
    ushort port; // client (RESP) port
    ushort raftPort;
    string dir;
    string conf;
    string lock; // per-node lockfile; a SIGKILLed node leaves it stale, so the
    // harness cuts it before every (re)start — else dreads refuses the port
    Pid pid;
    bool running;
    bool paused;
    bool member; // is this node a voting member of the raft config right now?
    bool founding; // one of the initial bootstrap members
}

final class Cluster
{
    Node[] nodes; // fixed pool of slots (founding members + spare join slots)
    string binary;
    string root;
    private ushort basePort_;
    private ushort baseRaft_;

    // `n` founding members + spare slots up to `pool` for servers to join into.
    this(string binary, string root, uint n, ushort basePort, ushort baseRaft, uint pool = 5)
    {
        this.binary = binary;
        this.root = root;
        this.basePort_ = basePort;
        this.baseRaft_ = baseRaft;
        if (pool < n)
            pool = n;
        if (root.exists)
            rmdirRecurse(root);
        mkdirRecurse(root);
        foreach (i; 0 .. pool)
        {
            Node nd;
            nd.id = i + 1;
            nd.port = cast(ushort)(basePort + i);
            nd.raftPort = cast(ushort)(baseRaft + i);
            nd.dir = format("%s/n%d", root, nd.id);
            nd.conf = format("%s/n%d.conf", root, nd.id);
            nd.lock = format("%s/dreads.lck", nd.dir);
            nd.founding = i < n;
            nd.member = i < n;
            mkdirRecurse(nd.dir);
            nodes ~= nd;
        }
        // founding members bootstrap knowing each other; spares are configured
        // as joiners (raft-join) at join time.
        foreach (ref nd; nodes)
            if (nd.founding)
                writeConf(nd, false);
    }

    // Raft addresses of the current voting members other than `self`.
    private string memberPeers(uint selfId)
    {
        string peers;
        foreach (ref other; nodes)
            if (other.member && other.id != selfId)
            {
                if (peers.length)
                    peers ~= ",";
                peers ~= format("%d@127.0.0.1:%d", other.id, other.raftPort);
            }
        return peers;
    }

    private void writeConf(ref Node nd, bool join)
    {
        // synchronous full: an acked write must be fdatasync'd before the reply,
        // so "acknowledged == durable" holds and a SIGKILL cannot lose a committed
        // write. (Without it the raft log sits in a userspace stdio buffer and a
        // crash loses it — the no-loss invariant would be meaningless.)
        auto c = format("port %d\ndir %s\nraft-node-id %d\nraft-port %d\nraft-peers %s\n"
            ~ "appendfilename dreads\nsynchronous full\n%s",
            nd.port, nd.dir, nd.id, nd.raftPort, memberPeers(nd.id),
            join ? "raft-join yes\n" : "");
        write(nd.conf, c);
    }

    // --- dynamic membership: servers entering and leaving ---

    // Bring a spare slot up as a learner and RAFT ADDNODE it into the cluster.
    // Returns the joined node's id, or 0 if no spare / no leader.
    uint joinServer()
    {
        int slot = -1;
        foreach (i, ref nd; nodes)
            if (!nd.member && !nd.running)
            {
                slot = cast(int) i;
                break;
            }
        if (slot < 0)
            return 0;
        auto lp = findLeaderPort();
        if (lp == 0)
            return 0;
        auto nd = &nodes[slot];
        // fresh state: a rejoining slot must not carry stale raft/aof from a
        // previous incarnation.
        try
            foreach (f; ["dreads.raftlog", "dreads.raftmeta", "dreads.raftlog.old",
                    "dreads.aof", "dreads.lck"])
                if (format("%s/%s", nd.dir, f).exists)
                    remove(format("%s/%s", nd.dir, f));
        catch (Exception)
        {
        }
        writeConf(*nd, true); // raft-join yes, peers = current members
        start(*nd);
        Thread.sleep(600.msecs); // let it come up as a learner
        auto cl = new RespClient(lp);
        auto r = cl.cmd("RAFT", "ADDNODE", format("%d@127.0.0.1:%d", nd.id, nd.raftPort));
        cl.close();
        if (r.kind == '+')
        {
            nd.member = true;
            // Now that it IS a committed member, rewrite its config WITHOUT
            // raft-join: joinMode is a first-boot-only thing (start as a passive
            // learner). A later restart must boot it as a NORMAL member and
            // recover its config/log — else it comes back a learner and never
            // rejoins/applies, stranding it behind forever.
            writeConf(*nd, false);
            return nd.id;
        }
        return 0; // ADDNODE rejected (leader changed / in flight); leave it spare
    }

    // RAFT REMOVENODE a member, then kill its process.
    uint leaveServer()
    {
        // pick a removable member (prefer a joined one; never drop below 3 voters)
        int slot = -1;
        int voters = memberCount();
        if (voters <= 3)
            return 0;
        foreach (i, ref nd; nodes)
            if (nd.member && !nd.founding && nd.running)
            {
                slot = cast(int) i;
                break;
            }
        if (slot < 0)
            return 0;
        auto lp = findLeaderPort();
        if (lp == 0)
            return 0;
        auto nd = &nodes[slot];
        auto cl = new RespClient(lp);
        auto r = cl.cmd("RAFT", "REMOVENODE", format("%d", nd.id));
        cl.close();
        if (r.kind != '+')
            return 0;
        nd.member = false;
        Thread.sleep(400.msecs); // let the config change commit before we kill it
        killNode(*nd);
        return nd.id;
    }

    int memberCount()
    {
        int c;
        foreach (ref nd; nodes)
            if (nd.member)
                c++;
        return c;
    }

    void start(ref Node nd)
    {
        if (nd.running)
            return;
        auto logf = File(format("%s/out.log", nd.dir), "a");
        // explicit --lockfile in a writable path: keeps the flock port-guard
        // ALWAYS active (the default /var/run path may be unwritable, silently
        // disabling it) so a co-binding zombie can never split client traffic.
        nd.pid = spawnProcess([binary, nd.conf, "--lockfile=" ~ nd.lock],
            std.stdio.stdin, logf, logf);
        nd.running = true;
        nd.paused = false;
    }

    void startAll()
    {
        foreach (ref nd; nodes)
            if (nd.founding)
                start(nd);
    }

    // Kill a node and WAIT for it to actually die before marking it down — a
    // still-alive process would let a same-port restart co-bind (SO_REUSEPORT)
    // and split traffic (a zombie). `graceful` sends SIGTERM (clean shutdown);
    // otherwise SIGKILL (an abrupt crash — the common case in the real world).
    // A graceful stop that stalls is escalated to SIGKILL so the harness never
    // hangs and never leaves a zombie.
    void killNode(ref Node nd, bool graceful = false)
    {
        if (!nd.running)
            return;
        try
        {
            if (nd.paused)
                kill(nd.pid, SIGCONT); // a SIGSTOP'd process can't act on SIGTERM
            kill(nd.pid, graceful ? SIGTERM : SIGKILL);
            bool reaped = false;
            foreach (i; 0 .. (graceful ? 40 : 50)) // graceful: up to ~4s, then SIGKILL
            {
                if (tryWait(nd.pid).terminated)
                {
                    reaped = true;
                    break;
                }
                if (graceful && i == 30)
                    kill(nd.pid, SIGKILL); // escalate a stalled graceful stop
                Thread.sleep(100.msecs);
            }
            if (!reaped)
            {
                kill(nd.pid, SIGKILL);
                wait(nd.pid);
            }
        }
        catch (Exception)
        {
        }
        nd.running = false;
        nd.paused = false;
    }

    void pauseNode(ref Node nd)
    {
        if (nd.running && !nd.paused)
        {
            try
                kill(nd.pid, SIGSTOP);
            catch (Exception)
            {
            }
            nd.paused = true;
        }
    }

    void resumeNode(ref Node nd)
    {
        if (nd.running && nd.paused)
        {
            try
                kill(nd.pid, SIGCONT);
            catch (Exception)
            {
            }
            nd.paused = false;
        }
    }

    void stopAll()
    {
        foreach (ref nd; nodes)
        {
            if (nd.running && nd.paused)
                resumeNode(nd);
            if (nd.running)
            {
                try
                {
                    kill(nd.pid, SIGTERM);
                }
                catch (Exception)
                {
                }
            }
        }
        Thread.sleep(300.msecs);
        foreach (ref nd; nodes)
            killNode(nd);
    }

    // returns the node index that currently accepts a write (the leader), or -1
    int findLeader(Duration budget = 8.seconds)
    {
        auto sw = StopWatch(AutoStart.yes);
        while (sw.peek < budget)
        {
            foreach (i, ref nd; nodes)
            {
                if (!nd.running || nd.paused)
                    continue;
                auto cl = new RespClient(nd.port);
                if (!cl.ok)
                    continue;
                auto r = cl.cmd("SET", "__leader_probe__", "1");
                cl.close();
                if (r.kind == '+' || r.kind == ':')
                    return cast(int) i;
            }
            Thread.sleep(150.msecs);
        }
        return -1;
    }
}

// ---------------------------------------------------------------------------
// shared chaos state
// ---------------------------------------------------------------------------

import core.atomic : atomicOp, atomicLoad, atomicStore;
import core.sync.mutex : Mutex;
import std.random : Random, uniform, unpredictableSeed;

__gshared Cluster gCluster;
__gshared bool gStop;
__gshared bool gChaosOn; // true while the chaos window is active
__gshared ushort[] gPorts; // all client ports (fixed; workers probe these)
shared long gAcked; // writes that got a success reply (committed)
shared long gFailed; // writes that failed/timed out (ambiguous)
shared long gAckedDuringChaos;
__gshared long[] gMaxSeq; // per-worker highest contiguously-acked SET seq (owned slot)
__gshared Mutex gClusterLock;

// Find a node that accepts writes (the leader). Probes the fixed port list, so
// it never races the chaos thread's Node-state mutations.
ushort findLeaderPort()
{
    foreach (p; gPorts)
    {
        auto cl = new RespClient(p, 400.msecs);
        if (!cl.ok)
            continue;
        auto r = cl.cmd("SET", "__probe__", "1");
        cl.close();
        if (r.kind == '+' || r.kind == ':')
            return p;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// client worker: idempotent SET oracle (retried => contiguous prefix, no dup)
// interleaved with varied datatype writes for coverage.
// ---------------------------------------------------------------------------

void workerMain(int id)
{
    RespClient conn;
    ushort port;
    long seq;

    void reconnect()
    {
        if (conn !is null)
            conn.close();
        port = findLeaderPort();
        conn = port ? new RespClient(port) : null;
    }

    reconnect();
    while (!atomicLoad(gStop))
    {
        if (conn is null || !conn.ok)
        {
            reconnect();
            if (conn is null)
            {
                Thread.sleep(50.msecs);
                continue;
            }
        }

        // --- oracle write: idempotent SET, retried on the same seq until it
        // commits (or we give up) so acked seqs stay a gap-free prefix ---
        auto key = format("w:%d:%d", id, seq);
        auto val = seq.to!string;
        bool committed = false;
        foreach (attempt; 0 .. 6)
        {
            if (conn is null || !conn.ok)
                reconnect();
            if (conn is null)
            {
                Thread.sleep(30.msecs);
                continue;
            }
            auto r = conn.cmd("SET", key, val);
            if (r.kind == '+')
            {
                committed = true;
                break;
            }
            // -READONLY (not leader) or transport error => re-find the leader
            reconnect();
            Thread.sleep(20.msecs);
        }
        if (committed)
        {
            gMaxSeq[id] = seq; // contiguous: seq committed, all prior committed
            atomicOp!"+="(gAcked, 1);
            if (atomicLoad(gChaosOn))
                atomicOp!"+="(gAckedDuringChaos, 1);
            seq++;
        }
        else
            atomicOp!"+="(gFailed, 1);

        // --- coverage writes: varied datatypes (best-effort, not oracle'd;
        // the convergence check validates them across nodes) ---
        if (conn !is null && conn.ok && (seq & 3) == 0)
        {
            auto s = seq.to!string;
            conn.cmd("HSET", format("h:%d", id), format("f%d", seq), s);
            conn.cmd("SADD", format("s:%d", id), s);
            conn.cmd("ZADD", format("z:%d", id), s, format("m%d", seq));
            conn.cmd("INCR", format("cnt:%d", id));
            conn.cmd("APPEND", format("a:%d", id), "x");
        }

        // client churn: occasionally drop the connection and reconnect
        if ((seq % 37) == 0)
            reconnect();
    }
    if (conn !is null)
        conn.close();
}

// ---------------------------------------------------------------------------
// chaos driver: worst-case, but usually keep a majority so liveness is testable
// ---------------------------------------------------------------------------

__gshared bool gChaosVerbose;

void clog(string s)
{
    if (gChaosVerbose)
    {
        stderr.writeln("  [chaos] ", s);
        stderr.flush();
    }
}

void chaosMain()
{
    auto rng = Random(unpredictableSeed ^ 0x9E3779B9);

    // Restore ONE downed/frozen MEMBER (resume a frozen one first, else restart a
    // dead member). Spare/removed slots are left alone — a node that LEFT the
    // cluster stays gone; only current members self-heal.
    uint restoreOne()
    {
        foreach (ref nd; gCluster.nodes)
            if (nd.member && nd.running && nd.paused)
            {
                gCluster.resumeNode(nd);
                clog(format("resume node %d", nd.id));
                return nd.id;
            }
        foreach (ref nd; gCluster.nodes)
            if (nd.member && !nd.running)
            {
                gCluster.start(nd);
                clog(format("restart node %d (same port %d)", nd.id, nd.port));
                return nd.id;
            }
        return 0;
    }

    int leaderIdx()
    {
        auto lp = findLeaderPort();
        foreach (i, ref nd; gCluster.nodes)
            if (nd.port == lp && nd.running && !nd.paused)
                return cast(int) i;
        return -1;
    }

    void act()
    {
        synchronized (gClusterLock)
        {
            // Majority is over the CURRENT voting config (which grows/shrinks).
            int voters = gCluster.memberCount();
            int majority = voters / 2 + 1;
            int up, downOrFrozen;
            int[] upCand;
            foreach (i, ref nd; gCluster.nodes)
                if (nd.member)
                {
                    if (nd.running && !nd.paused)
                    {
                        up++;
                        upCand ~= cast(int) i;
                    }
                    else
                        downOrFrozen++;
                }

            // Self-heal bias: a downed/frozen MEMBER is usually brought back, so
            // the cluster continuously re-forms and catches laggards up. Strong
            // bias keeps a majority available most of the time (availability).
            if (downOrFrozen > 0 && uniform(0, 100, rng) < 72)
            {
                restoreOne();
                return;
            }
            // Keep one node of slack above the majority floor: disrupting down TO
            // the floor leaves no margin, so a single lagging heal causes a stall.
            // Restoring here trades a bit of "worst case" for staying available.
            if (up <= majority + (voters >= 5 ? 1 : 0) || up <= majority)
            {
                restoreOne();
                return;
            }

            immutable graceful = uniform(0, 100, rng) < 25;
            immutable how = graceful ? "graceful" : "abrupt";
            auto roll = uniform(0, 100, rng);

            // --- servers ENTERING and LEAVING (dynamic membership) ---
            if (roll < 10)
            {
                auto jid = gCluster.joinServer(); // learner + RAFT ADDNODE
                if (jid)
                {
                    clog(format("JOIN server %d (RAFT ADDNODE)", jid));
                    return;
                }
            }
            else if (roll < 18)
            {
                auto rid = gCluster.leaveServer(); // RAFT REMOVENODE + kill
                if (rid)
                {
                    clog(format("LEAVE server %d (RAFT REMOVENODE)", rid));
                    return;
                }
            }

            // --- FREEZE (dominant): dropped connection / lost route / stall ---
            if (roll < 50)
            {
                auto li = leaderIdx();
                if (li >= 0)
                {
                    gCluster.pauseNode(gCluster.nodes[li]);
                    clog(format("FREEZE LEADER node %d (SIGSTOP — lost route)", gCluster.nodes[li].id));
                    return;
                }
            }
            if (roll < 72 && upCand.length)
            {
                auto k = upCand[uniform(0, cast(int) upCand.length, rng)];
                gCluster.pauseNode(gCluster.nodes[k]);
                clog(format("freeze node %d (SIGSTOP — dropped conn)", gCluster.nodes[k].id));
                return;
            }

            // --- KILL THE LEADER: frequent, so failovers are constant. A leader
            // killed within its first ~RTT of election dies BEFORE committing its
            // NOOP — the "master that never synced, dying half-way". ---
            if (roll < 88)
            {
                auto li = leaderIdx();
                if (li >= 0)
                {
                    gCluster.killNode(gCluster.nodes[li], graceful);
                    clog(format("KILL LEADER node %d [%s]", gCluster.nodes[li].id, how));
                    return;
                }
            }
            // --- kill a random member (abrupt or graceful) ---
            if (upCand.length)
            {
                auto k = upCand[uniform(0, cast(int) upCand.length, rng)];
                gCluster.killNode(gCluster.nodes[k], graceful);
                clog(format("kill node %d [%s]", gCluster.nodes[k].id, how));
            }
        }
    }

    while (!atomicLoad(gStop))
    {
        act();
        Thread.sleep(uniform(250, 900, rng).msecs);
    }
}

// ---------------------------------------------------------------------------
// main orchestration
// ---------------------------------------------------------------------------

int main(string[] args)
{
    auto binary = "./bin/dreads";
    if (!binary.exists)
    {
        stderr.writeln("chaos: ./bin/dreads not found — build it (dub build -b release)");
        return 1;
    }
    int durationSec = args.length > 1 ? args[1].to!int : 30;
    uint n = args.length > 2 ? args[2].to!uint : 3;
    // Data dir MUST be on a real disk, not tmpfs: /tmp is RAM-backed on most
    // systems, so a SIGKILLed process's page-cached writes survive there and a
    // durability bug would hide. /var/tmp is persistent (ext4/xfs).
    auto root = "/var/tmp/dreads-chaos";
    int nWorkers = 8;

    gClusterLock = new Mutex;
    gCluster = new Cluster(binary, root, n, 7100, 17100);
    gPorts = null;
    foreach (ref nd; gCluster.nodes)
        gPorts ~= nd.port;
    gMaxSeq = new long[nWorkers];
    gMaxSeq[] = -1;

    writeln("chaos: starting ", n, "-node real raft cluster + ", nWorkers, " client workers ...");
    gCluster.startAll();
    scope (exit)
        gCluster.stopAll();

    if (gCluster.findLeader() < 0)
    {
        stderr.writeln("chaos: FAIL — cluster never elected an initial leader");
        return 2;
    }
    writeln("chaos: initial leader up. Unleashing workers + chaos for ", durationSec, "s ...");

    Thread[] workers;
    foreach (i; 0 .. nWorkers)
    {
        auto wid = i;
        auto t = new Thread({ workerMain(wid); });
        t.start();
        workers ~= t;
    }
    // warm up briefly with no chaos so a baseline of writes lands
    Thread.sleep(1500.msecs);
    auto ackedBefore = atomicLoad(gAcked);

    import std.process : environment;

    gChaosVerbose = environment.get("CHAOS_VERBOSE", "").length > 0;
    atomicStore(gChaosOn, true);
    auto chaos = new Thread({ chaosMain(); });
    chaos.start();

    // Run the chaos window, sampling availability every second: a second with
    // zero new acked writes is a "stall" (the cluster was briefly unavailable).
    // We track the longest consecutive stall and the fraction of seconds that
    // made progress — the "keeps working DURING chaos" goal.
    auto sw = StopWatch(AutoStart.yes);
    long prevAcked = atomicLoad(gAcked);
    int progressSecs, stallSecs, maxStall, curStall, totalSecs;
    while (sw.peek < durationSec.seconds)
    {
        Thread.sleep(1.seconds);
        auto now = atomicLoad(gAcked);
        totalSecs++;
        if (now > prevAcked)
        {
            progressSecs++;
            curStall = 0;
        }
        else
        {
            stallSecs++;
            curStall++;
            if (curStall > maxStall)
                maxStall = curStall;
        }
        prevAcked = now;
        writef("\r  t=%2ds  acked=%d  failed=%d  (during-chaos acked=%d)  maxStall=%ds   ",
            cast(int) sw.peek.total!"seconds", now,
            atomicLoad(gFailed), atomicLoad(gAckedDuringChaos), maxStall);
        stdout.flush();
    }
    writeln();

    // --- heal: stop chaos, resume+restart everything, let it converge ---
    atomicStore(gChaosOn, false);
    atomicStore(gStop, true);
    chaos.join();
    foreach (t; workers)
        t.join();

    writeln("chaos: healing — resume/restart all MEMBER nodes, wait for convergence ...");
    synchronized (gClusterLock)
    {
        foreach (ref nd; gCluster.nodes)
        {
            if (!nd.member)
                continue; // spares / left nodes stay down
            if (nd.running && nd.paused)
                gCluster.resumeNode(nd);
            if (!nd.running)
                gCluster.start(nd);
        }
    }

    auto leaderPort = healWaitLeader(20.seconds);
    if (leaderPort == 0)
    {
        stderr.writeln("chaos: FAIL — cluster did not recover a leader after chaos");
        return 3;
    }

    // Wait for convergence: nodes that JOINED late catch up via log/snapshot,
    // which takes time. Poll every member's DBSIZE until they all agree (or a
    // generous timeout), so the check measures true divergence, not catch-up lag.
    writeln("chaos: waiting for all members to converge ...");
    {
        auto cw = StopWatch(AutoStart.yes);
        while (cw.peek < 30.seconds)
        {
            long[] szs;
            bool anyDown = false;
            foreach (ref nd; gCluster.nodes)
            {
                if (!nd.member)
                    continue;
                auto c = new RespClient(nd.port, 2000.msecs);
                if (!c.ok)
                {
                    anyDown = true;
                    c.close();
                    continue;
                }
                auto r = c.cmd("DBSIZE");
                c.close();
                szs ~= (r.kind == ':') ? r.num : -1;
            }
            bool allEqual = szs.length > 0;
            foreach (s; szs)
                if (s < 0 || s != szs[0])
                    allEqual = false;
            if (allEqual && !anyDown)
                break;
            Thread.sleep(1.seconds);
        }
    }

    // ===================== VERIFY SAFETY =====================
    bool ok = true;

    // (1) no acked write lost: every contiguous SET prefix must be present
    long totalAcked, lost;
    auto lc = new RespClient(leaderPort, 3000.msecs);
    foreach (wid; 0 .. nWorkers)
    {
        auto maxSeq = gMaxSeq[wid];
        for (long s = 0; s <= maxSeq; s++)
        {
            totalAcked++;
            auto r = lc.cmd("GET", format("w:%d:%d", wid, s));
            if (!(r.kind == '$' && r.str == s.to!string))
            {
                lost++;
                if (lost <= 5)
                    stderr.writeln("  LOST acked write w:", wid, ":", s,
                        " -> ", r.kind, r.str);
            }
        }
    }
    lc.close();

    // (2) convergence: every current MEMBER agrees on DBSIZE. Also capture each
    // node's view of the leader, so a divergence can be diagnosed (a stuck
    // follower behind on replication vs. a confused/partitioned node).
    long[] sizes;
    string[] diag;
    foreach (ref nd; gCluster.nodes)
    {
        if (!nd.member)
            continue;
        auto c = new RespClient(nd.port, 3000.msecs);
        if (!c.ok)
        {
            sizes ~= -1;
            diag ~= format("n%d=DOWN", nd.id);
            continue;
        }
        auto sz = c.cmd("DBSIZE");
        auto ldr = c.cmd("RAFT", "LEADER"); // int: which node this one thinks leads
        c.close();
        auto s = (sz.kind == ':') ? sz.num : -1;
        sizes ~= s;
        diag ~= format("n%d(%s) dbsize=%d sees-leader=%s", nd.id,
            nd.founding ? "founding" : "joined", s,
            ldr.kind == ':' ? ldr.num.to!string : "?");
    }
    bool converged = true;
    long ref0 = -1;
    foreach (sz; sizes)
        if (sz >= 0)
        {
            if (ref0 < 0)
                ref0 = sz;
            else if (sz != ref0)
                converged = false;
        }

    writeln();
    writeln("================= CHAOS REPORT =================");
    writeln(" acked writes (committed)   : ", atomicLoad(gAcked));
    writeln(" acked DURING chaos         : ", atomicLoad(gAckedDuringChaos));
    writeln(" failed/ambiguous attempts  : ", atomicLoad(gFailed));
    writeln(" acked before chaos         : ", ackedBefore);
    writeln(" oracle keys checked        : ", totalAcked);
    writeln(" LOST acked writes          : ", lost, lost == 0 ? "  OK" : "  <-- SAFETY VIOLATION");
    writeln(" per-node DBSIZE            : ", sizes);
    writeln(" final members              : ", gCluster.memberCount());
    foreach (d; diag)
        writeln("   ", d);
    writeln(" convergence                : ", converged ? "OK" : "DIVERGED  <-- SAFETY VIOLATION");
    // Availability DURING chaos: what fraction of seconds made write progress,
    // and the longest stall (a majority loss / failover gap). "Keeps working
    // during chaos" = high availability, short worst-case stall.
    auto availPct = totalSecs > 0 ? (100 * progressSecs) / totalSecs : 0;
    writeln(" availability DURING chaos  : ", progressSecs, "/", totalSecs,
        " s made progress (", availPct, "%),  longest stall ", maxStall, "s");
    // A majority loss / election is expected to cause SHORT stalls; a long stall
    // means the cluster wedged. Under relentless total chaos (kills+freezes+
    // membership every ~sub-second), allow a few election timeouts of gap.
    bool available = maxStall <= 8 && availPct >= 55;
    writeln(" liveness                   : ", available ? "OK"
        : "DEGRADED  <-- long unavailability during chaos");
    writeln("===============================================");

    if (lost != 0 || !converged)
        ok = false;
    if (!available)
        ok = false;
    return ok ? 0 : 4;
}

// after healing, wait until a leader is back AND replication has quiesced
ushort healWaitLeader(Duration budget)
{
    auto sw = StopWatch(AutoStart.yes);
    while (sw.peek < budget)
    {
        auto p = findLeaderPort();
        if (p != 0)
        {
            Thread.sleep(2.seconds); // let followers catch up / snapshots land
            return p;
        }
        Thread.sleep(300.msecs);
    }
    return 0;
}
