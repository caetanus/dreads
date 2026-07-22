// Bytecode-IR spike (EXPERIMENT — go/no-go, not production).
//
// Question: does a RESP→bytecode compiler (db+key first, command as an opcode byte)
// actually pay off? It would REPLACE the per-command work the server does today:
//   - parse the RESP once, but on the SHARDING hop the OWNER RE-PARSES the raw bytes;
//   - resolve the command NAME to an identity ~4x per command: the executeCommand
//     switch, the dispatch() switch, aclCmdIndex (stats), commandRouteKey (routing) —
//     each an uppercase/lowercase pass + a string switch over ~300 command names.
// Bytecode resolves ONCE (opcode) and everyone indexes by it; the owner executes the
// bytecode with NO re-parse.
//
// This measures the ELIMINABLE pieces in DETERMINISTIC instructions/op (no network, so
// this machine's throughput noise is irrelevant). Run each phase at N and 2N iters under
// `perf stat -e instructions`; ins/op = (I(2N) - I(N)) / (N * WL.length) removes fixed
// startup cost. Then weigh against a full SET (~3000-4300 ins/op on record) to decide.
//
// Build: dub build -b release --config=bytecode-spike --compiler=ldc2
// Run:   perf stat -e instructions bin/bytecode_spike <parse|resolve|route|opdecode> <iters>

import core.stdc.stdio : printf;
import core.stdc.stdlib : strtol;
import dreads.resp : parseValue, RVal, RType, ParseStatus;
import dreads.mem : Arena, ByteBuffer;
import dreads.acl : aclCmdIndex, commandRouteKey;
import dreads.obj : Keyspace;
import dreads.commands : dispatch;

// A realistic command mix as raw RESP bytes (the wire form the server receives).
static immutable string[] WL = [
    "*3\r\n$3\r\nSET\r\n$8\r\nkey:1234\r\n$3\r\nval\r\n",
    "*2\r\n$3\r\nGET\r\n$8\r\nkey:5678\r\n",
    "*2\r\n$4\r\nINCR\r\n$7\r\ncnt:900\r\n",
    "*4\r\n$4\r\nHSET\r\n$6\r\nhash:1\r\n$1\r\nf\r\n$1\r\nv\r\n",
    "*3\r\n$5\r\nLPUSH\r\n$6\r\nlist:2\r\n$3\r\nxyz\r\n",
    "*3\r\n$6\r\nEXPIRE\r\n$8\r\nkey:1234\r\n$3\r\n100\r\n",
];
// lowercase command names (aclCmdIndex/commandRouteKey take LOWERCASE), same order.
static immutable string[] LNAME = ["set", "get", "incr", "hset", "lpush", "expire"];

private Arena gPerm; // never reset: keeps the pre-parsed RVal arrays alive
private Arena gScratch; // reset per command in the parse phase

void main(string[] args) @system
{
    if (args.length < 3)
    {
        printf("usage: bytecode_spike <parse|resolve|route|opdecode> <iters>\n");
        return;
    }
    immutable phase = args[1];
    immutable iters = cast(size_t) strtol(args[2].ptr, null, 10);

    // pre-parse the workload once (for the route phase, which needs parsed RVal arrays).
    RVal[WL.length] parsed;
    foreach (i, w; WL)
    {
        size_t pos = 0;
        cast(void) parseValue(cast(const(ubyte)[]) w, pos, gPerm, parsed[i]);
    }
    // the bytecode's opcode = the command index, resolved ONCE at (hypothetical) compile.
    ubyte[WL.length] opcode;
    foreach (i; 0 .. WL.length)
        opcode[i] = cast(ubyte) aclCmdIndex(LNAME[i]);

    ulong sink;
    if (phase == "parse")
    {
        // what the OWNER re-does on every cross-shard hop (and every command parses once).
        foreach (it; 0 .. iters)
            foreach (w; WL)
            {
                RVal v;
                size_t pos = 0;
                gScratch.reset();
                cast(void) parseValue(cast(const(ubyte)[]) w, pos, gScratch, v);
                sink += v.arr.length;
            }
    }
    else if (phase == "resolve")
    {
        // ONE of the ~4 per-command name→identity resolutions (a string switch over ~300
        // names). Bytecode does this ZERO times downstream (the opcode IS the identity).
        foreach (it; 0 .. iters)
            foreach (i; 0 .. WL.length)
                sink += aclCmdIndex(LNAME[i]);
    }
    else if (phase == "route")
    {
        // routing: extract the owning-shard key. Bytecode reads it at a FIXED offset.
        foreach (it; 0 .. iters)
            foreach (i; 0 .. WL.length)
            {
                auto k = commandRouteKey(LNAME[i], parsed[i].arr);
                sink += k.length;
            }
    }
    else if (phase == "opdecode")
    {
        // the bytecode REPLACEMENT for resolve+route: opcode byte is the identity (jump
        // table index), key is at a known offset — a load, not a scan.
        foreach (it; 0 .. iters)
            foreach (i; 0 .. WL.length)
                sink += opcode[i];
    }
    else if (phase == "full")
    {
        // the REAL denominator: parse + dispatch a full SET then GET into a live keyspace
        // (overwrite/read — no unbounded growth), measured on THIS build. ins/op here is
        // the whole per-command cost the savings above are a fraction of.
        static Keyspace ks;
        static ByteBuffer o;
        static Arena a;
        static immutable string setCmd = WL[0]; // SET key:1234 val
        static immutable string getCmd = WL[1]; // GET key:5678
        foreach (it; 0 .. iters)
        {
            foreach (w; [setCmd, getCmd])
            {
                RVal v;
                size_t pos = 0;
                a.reset();
                o.clear();
                cast(void) parseValue(cast(const(ubyte)[]) w, pos, a, v);
                cast(void) dispatch(v, ks, o, a);
                sink += o.length;
            }
        }
        // /2 later: this phase runs 2 commands per iter, not WL.length.
    }
    else
    {
        printf("unknown phase\n");
        return;
    }
    printf("phase=%s iters=%zu sink=%llu\n", args[1].ptr, iters, sink);
}
