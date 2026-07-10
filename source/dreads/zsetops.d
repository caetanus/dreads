module dreads.zsetops;

// Sorted-set algebra and the full range-command family: one collection core
// serves ZRANGE (unified BYSCORE/BYLEX/REV/LIMIT), the legacy BY* variants,
// ZRANGESTORE, ZLEXCOUNT/ZREMRANGEBYLEX, ZRANDMEMBER, ZMPOP and the
// store/non-store union/inter/diff commands (which also accept plain sets,
// scored as 1, like Redis).

import dreads.commands : eqICKeyword, normalizeRange, parseDouble, parseLong,
    parseScoreBound, repDouble;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.notify : notifyKeyspaceEvent, NClass;
import dreads.resp;

private struct MS
{
    const(char)[] m;
    double s;
}

private struct LexBound
{
    bool negInf, posInf, excl;
    const(char)[] v;
}

private bool parseLexBound(scope const(char)[] s, out LexBound b) @nogc nothrow
{
    if (s == "-")
    {
        b.negInf = true;
        return true;
    }
    if (s == "+")
    {
        b.posInf = true;
        return true;
    }
    if (s.length >= 1 && (s[0] == '[' || s[0] == '('))
    {
        b.excl = s[0] == '(';
        b.v = s[1 .. $];
        return true;
    }
    return false;
}

private void repWrongTypeZ(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

// ---------------------------------------------------------------------------
// Range core
// ---------------------------------------------------------------------------

private enum RangeMode
{
    index,
    score,
    lex
}

private struct RangeSpec
{
    RangeMode mode;
    bool rev;
    long start, stop; // index mode
    double smin, smax; // score mode
    bool sminX, smaxX;
    LexBound lmin, lmax; // lex mode
    long limitOff = 0;
    long limitCnt = -1; // -1 = unlimited
}

/// Collects the matching (member, score) pairs in reply order.
private MS[] collectRange(RObj* obj, const ref RangeSpec sp, ref Arena arena) @nogc nothrow
{
    if (obj is null)
        return null;
    auto len = obj.zset.length;
    auto buf = arena.allocArray!MS(len);
    size_t n = 0;
    final switch (sp.mode)
    {
    case RangeMode.index:
        {
            long start = sp.start, stop = sp.stop;
            normalizeRange(start, stop, cast(long) len);
            if (start > stop)
                return null;
            obj.zset.walkRange(cast(size_t) start, cast(size_t)(stop - start + 1), sp.rev, (m, s) {
                buf[n++] = MS(m, s);
                return 0;
            });
            return buf[0 .. n];
        }
    case RangeMode.score:
        obj.zset.walkScoreRange(sp.smin, sp.sminX, sp.smax, sp.smaxX, (m, s) {
            buf[n++] = MS(m, s);
            return 0;
        });
        break;
    case RangeMode.lex:
        obj.zset.walkLexRange(sp.lmin.v, sp.lmin.excl, sp.lmin.negInf,
                sp.lmax.v, sp.lmax.excl, sp.lmax.posInf, (m, s) {
            buf[n++] = MS(m, s);
            return 0;
        });
        break;
    }
    auto hits = buf[0 .. n];
    if (sp.rev) // collected ascending; reverse in place
    {
        foreach (i; 0 .. n / 2)
        {
            auto t = hits[i];
            hits[i] = hits[n - 1 - i];
            hits[n - 1 - i] = t;
        }
    }
    // LIMIT offset count (score/lex modes only)
    auto off = cast(size_t)(sp.limitOff < 0 ? 0 : sp.limitOff);
    if (off >= hits.length)
        return null;
    hits = hits[off .. $];
    if (sp.limitCnt >= 0 && hits.length > cast(size_t) sp.limitCnt)
        hits = hits[0 .. cast(size_t) sp.limitCnt];
    return hits;
}

/// Parses [BYSCORE|BYLEX] [REV] [LIMIT o c] [WITHSCORES] plus the bounds.
/// boundsRev: bounds arrive as (max, min) — the legacy ZREV* forms.
private bool parseRangeArgs(const(RVal)[] bounds, const(RVal)[] opts, bool boundsRev,
        ref RangeSpec sp, ref bool withScores, ref bool limitUsed) @nogc nothrow
{
    size_t i = 0;
    while (i < opts.length)
    {
        auto w = opts[i].str;
        if (eqICKeyword(w, "BYSCORE"))
        {
            sp.mode = RangeMode.score;
            i++;
        }
        else if (eqICKeyword(w, "BYLEX"))
        {
            sp.mode = RangeMode.lex;
            i++;
        }
        else if (eqICKeyword(w, "REV"))
        {
            sp.rev = true;
            i++;
        }
        else if (eqICKeyword(w, "WITHSCORES"))
        {
            withScores = true;
            i++;
        }
        else if (eqICKeyword(w, "LIMIT") && i + 2 < opts.length + 1 && i + 2 <= opts.length)
        {
            if (i + 2 > opts.length || !parseLong(opts[i + 1].str, sp.limitOff)
                    || !parseLong(opts[i + 2].str, sp.limitCnt))
                return false;
            limitUsed = true;
            i += 3;
        }
        else
            return false;
    }
    // resolve bounds: when the command form is reversed, (first, second) is (max, min)
    auto first = bounds[0].str;
    auto second = bounds[1].str;
    auto minTok = (boundsRev || sp.rev) && sp.mode != RangeMode.index ? second : first;
    auto maxTok = (boundsRev || sp.rev) && sp.mode != RangeMode.index ? first : second;
    final switch (sp.mode)
    {
    case RangeMode.index:
        return parseLong(first, sp.start) && parseLong(second, sp.stop);
    case RangeMode.score:
        return parseScoreBound(minTok, sp.smin, sp.sminX)
            && parseScoreBound(maxTok, sp.smax, sp.smaxX);
    case RangeMode.lex:
        return parseLexBound(minTok, sp.lmin) && parseLexBound(maxTok, sp.lmax);
    }
}

/// form: 0 ZRANGE (unified), 1 ZREVRANGE, 2 ZRANGEBYSCORE, 3 ZREVRANGEBYSCORE,
/// 4 ZRANGEBYLEX, 5 ZREVRANGEBYLEX
public void zrangeGeneric(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, int form) @nogc nothrow
{
    if (args.length < 3)
    {
        repError(o, "ERR wrong number of arguments");
        return;
    }
    RangeSpec sp;
    bool withScores, limitUsed;
    bool boundsRev;
    switch (form)
    {
    case 1:
        sp.rev = true;
        break;
    case 2:
        sp.mode = RangeMode.score;
        break;
    case 3:
        sp.mode = RangeMode.score;
        sp.rev = true;
        boundsRev = true;
        break;
    case 4:
        sp.mode = RangeMode.lex;
        break;
    case 5:
        sp.mode = RangeMode.lex;
        sp.rev = true;
        boundsRev = true;
        break;
    default:
        break;
    }
    if (!parseRangeArgs(args[1 .. 3], args[3 .. $], boundsRev, sp, withScores, limitUsed))
    {
        repError(o, "ERR syntax error");
        return;
    }
    if (limitUsed && sp.mode == RangeMode.index)
    {
        repError(o,
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return;
    }
    if (withScores && sp.mode == RangeMode.lex)
    {
        repError(o, "ERR syntax error");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeZ(o);
        return;
    }
    auto hits = collectRange(obj, sp, arena);
    repArrayHeader(o, hits.length * (withScores ? 2 : 1));
    foreach (ref h; hits)
    {
        repBulk(o, h.m);
        if (withScores)
            repDouble(o, h.s);
    }
}

/// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT o c]
public void zrangestore(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 4)
    {
        repError(o, "ERR wrong number of arguments for 'zrangestore' command");
        return;
    }
    RangeSpec sp;
    bool withScores, limitUsed;
    if (!parseRangeArgs(args[2 .. 4], args[4 .. $], false, sp, withScores, limitUsed) || withScores)
    {
        repError(o, "ERR syntax error");
        return;
    }
    bool wrong;
    auto src = ks.lookupTyped(args[1].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeZ(o);
        return;
    }
    auto hits = collectRange(src, sp, arena);
    // dst may equal src: copy members before the keyspace mutates
    foreach (ref h; hits)
        h.m = arena.dupString(h.m);
    if (hits.length == 0)
    {
        if (ks.del(args[0].str))
            notifyKeyspaceEvent(NClass.generic, "del", args[0].str);
        repInt(o, 0);
        return;
    }
    RObj obj;
    obj.type = ObjType.zset;
    foreach (ref h; hits)
        obj.zset.add(h.s, h.m);
    ks.d.set(args[0].str, obj);
    notifyKeyspaceEvent(NClass.zset, "zrangestore", args[0].str);
    repInt(o, cast(long) hits.length);
}

/// ZLEXCOUNT key min max / ZREMRANGEBYLEX key min max
public void zlexRange(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, bool remove) @nogc nothrow
{
    if (args.length != 3)
    {
        repError(o, remove ? "ERR wrong number of arguments for 'zremrangebylex' command"
                : "ERR wrong number of arguments for 'zlexcount' command");
        return;
    }
    LexBound lmin, lmax;
    if (!parseLexBound(args[1].str, lmin) || !parseLexBound(args[2].str, lmax))
    {
        repError(o, "ERR min or max not valid string range item");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeZ(o);
        return;
    }
    if (obj is null)
    {
        repInt(o, 0);
        return;
    }
    auto victims = arena.allocArray!(const(char)[])(obj.zset.length);
    size_t n = 0;
    obj.zset.walkLexRange(lmin.v, lmin.excl, lmin.negInf, lmax.v, lmax.excl, lmax.posInf, (m, s) {
        if (remove)
            victims[n] = arena.dupString(m);
        n++;
        return 0;
    });
    if (remove)
    {
        foreach (m; victims[0 .. n])
            obj.zset.remove(m);
        if (n > 0)
            notifyKeyspaceEvent(NClass.zset, "zremrangebylex", args[0].str);
        ks.delIfEmpty(args[0].str, obj);
    }
    repInt(o, cast(long) n);
}

/// ZRANDMEMBER key [count [WITHSCORES]] — deterministic (first ranks).
public void zrandmember(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1 || args.length > 3)
    {
        repError(o, "ERR wrong number of arguments for 'zrandmember' command");
        return;
    }
    bool withScores = args.length == 3 && eqICKeyword(args[2].str, "WITHSCORES");
    if (args.length == 3 && !withScores)
    {
        repError(o, "ERR syntax error");
        return;
    }
    long count = 1;
    bool withCount = args.length >= 2;
    if (withCount && !parseLong(args[1].str, count))
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeZ(o);
        return;
    }
    if (obj is null || obj.zset.length == 0)
    {
        if (withCount)
            repArrayHeader(o, 0);
        else
            repNullBulk(o);
        return;
    }
    if (!withCount)
    {
        obj.zset.walkRange(0, 1, false, (m, s) { repBulk(o, m); return 0; });
        return;
    }
    bool repeat = count < 0;
    auto want = cast(size_t)(repeat ? -count : count);
    auto n = repeat ? want : (want < obj.zset.length ? want : obj.zset.length);
    repArrayHeader(o, n * (withScores ? 2 : 1));
    size_t emitted = 0;
    while (emitted < n)
    {
        obj.zset.walkRange(0, obj.zset.length, false, (m, s) {
            if (emitted == n)
                return 1;
            repBulk(o, m);
            if (withScores)
                repDouble(o, s);
            emitted++;
            return 0;
        });
    }
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
public void zmpop(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    long numkeys;
    if (args.length < 3 || !parseLong(args[0].str, numkeys) || numkeys < 1
            || args.length < 1 + cast(size_t) numkeys + 1)
    {
        repError(o, "ERR numkeys should be greater than 0");
        return;
    }
    auto keys = args[1 .. 1 + cast(size_t) numkeys];
    auto rest = args[1 + cast(size_t) numkeys .. $];
    bool popMax;
    if (eqICKeyword(rest[0].str, "MAX"))
        popMax = true;
    else if (!eqICKeyword(rest[0].str, "MIN"))
    {
        repError(o, "ERR syntax error");
        return;
    }
    long count = 1;
    if (rest.length == 3 && eqICKeyword(rest[1].str, "COUNT"))
    {
        if (!parseLong(rest[2].str, count) || count < 1)
        {
            repError(o, "ERR count should be greater than 0");
            return;
        }
    }
    else if (rest.length != 1)
    {
        repError(o, "ERR syntax error");
        return;
    }
    foreach (ref k; keys)
    {
        bool wrong;
        auto obj = ks.lookupTyped(k.str, ObjType.zset, wrong);
        if (wrong)
        {
            repWrongTypeZ(o);
            return;
        }
        if (obj is null || obj.zset.length == 0)
            continue;
        auto n = cast(size_t)(count < cast(long) obj.zset.length ? count
                : cast(long) obj.zset.length);
        repArrayHeader(o, 2);
        repBulk(o, k.str);
        repArrayHeader(o, n);
        foreach (_; 0 .. n)
        {
            const(char)[] victim;
            obj.zset.walkRange(0, 1, popMax, (m, s) {
                repArrayHeader(o, 2);
                repBulk(o, m);
                repDouble(o, s);
                victim = arena.dupString(m);
                return 0;
            });
            obj.zset.remove(victim);
        }
        ks.delIfEmpty(k.str, obj);
        return;
    }
    o.append("*-1\r\n");
}

// ---------------------------------------------------------------------------
// Union / inter / diff (accepting both sets and zsets as sources)
// ---------------------------------------------------------------------------

private enum Agg
{
    sum,
    min,
    max
}

/// Iterates (member, weighted score) of a zset or plain set (score 1).
private int eachMemberScore(RObj* obj, double weight,
        scope int delegate(const(char)[] m, double s) @nogc nothrow dg) @nogc nothrow
{
    if (obj is null)
        return 0;
    if (obj.type == ObjType.zset)
        return obj.zset.walkRange(0, obj.zset.length, false, (m, s) => dg(m, s * weight));
    foreach (i; 0 .. obj.set.capacity)
    {
        if (!obj.set.slotLive(i))
            continue;
        auto r = dg(obj.set.keyAt(i), weight);
        if (r)
            return r;
    }
    return 0;
}

private bool memberScore(RObj* obj, scope const(char)[] m, out double s) @nogc nothrow
{
    if (obj is null)
        return false;
    if (obj.type == ObjType.zset)
        return obj.zset.score(m, s);
    if (obj.set.exists(m))
    {
        s = 1;
        return true;
    }
    return false;
}

private double aggregate(Agg agg, double a, double b) @nogc nothrow
{
    final switch (agg)
    {
    case Agg.sum:
        return a + b;
    case Agg.min:
        return a < b ? a : b;
    case Agg.max:
        return a > b ? a : b;
    }
}

/// ZUNION/ZINTER/ZDIFF [STORE] and ZINTERCARD.
/// op: 'U', 'I', 'D'; mode: 0 = reply members, 1 = store to dst, 2 = card only.
public void zsetCombine(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, char op, int mode) @nogc nothrow
{
    size_t at = 0;
    const(char)[] dst;
    if (mode == 1)
    {
        if (args.length < 3)
        {
            repError(o, "ERR wrong number of arguments");
            return;
        }
        dst = args[0].str;
        at = 1;
    }
    long numkeys;
    if (args.length < at + 2 || !parseLong(args[at].str, numkeys) || numkeys < 1
            || args.length < at + 1 + cast(size_t) numkeys)
    {
        repError(o, "ERR at least 1 input key is needed");
        return;
    }
    auto keys = args[at + 1 .. at + 1 + cast(size_t) numkeys];
    auto opts = args[at + 1 + cast(size_t) numkeys .. $];

    auto weights = arena.allocArray!double(keys.length);
    weights[] = 1;
    Agg agg = Agg.sum;
    bool withScores;
    long cardLimit = 0;
    size_t i = 0;
    while (i < opts.length)
    {
        if (op != 'D' && mode != 2 && eqICKeyword(opts[i].str, "WEIGHTS"))
        {
            if (i + 1 + keys.length > opts.length)
            {
                repError(o, "ERR syntax error");
                return;
            }
            foreach (w; 0 .. keys.length)
            {
                if (!parseDouble(opts[i + 1 + w].str, weights[w]))
                {
                    repError(o, "ERR weight value is not a float");
                    return;
                }
            }
            i += 1 + keys.length;
        }
        else if (op != 'D' && mode != 2 && eqICKeyword(opts[i].str, "AGGREGATE")
                && i + 1 < opts.length)
        {
            if (eqICKeyword(opts[i + 1].str, "SUM"))
                agg = Agg.sum;
            else if (eqICKeyword(opts[i + 1].str, "MIN"))
                agg = Agg.min;
            else if (eqICKeyword(opts[i + 1].str, "MAX"))
                agg = Agg.max;
            else
            {
                repError(o, "ERR syntax error");
                return;
            }
            i += 2;
        }
        else if (mode == 0 && eqICKeyword(opts[i].str, "WITHSCORES"))
        {
            withScores = true;
            i++;
        }
        else if (mode == 2 && eqICKeyword(opts[i].str, "LIMIT") && i + 1 < opts.length)
        {
            if (!parseLong(opts[i + 1].str, cardLimit) || cardLimit < 0)
            {
                repError(o, "ERR LIMIT can't be negative");
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

    // resolve sources (set or zset both fine; anything else is WRONGTYPE)
    auto srcs = arena.allocArray!(RObj*)(keys.length);
    foreach (k; 0 .. keys.length)
    {
        auto obj = ks.lookup(keys[k].str);
        if (obj !is null && obj.type != ObjType.zset && obj.type != ObjType.set)
        {
            repWrongTypeZ(o);
            return;
        }
        srcs[k] = obj;
    }

    // accumulate into a temporary zset (result order = score order, as Redis)
    RObj acc;
    acc.type = ObjType.zset;
    scope (exit)
    {
        if (mode != 1)
            acc.free();
    }
    if (op == 'U')
    {
        foreach (k; 0 .. srcs.length)
        {
            eachMemberScore(srcs[k], weights[k], (m, s) {
                double cur;
                if (acc.zset.score(m, cur))
                    acc.zset.add(aggregate(agg, cur, s), m);
                else
                    acc.zset.add(s, m);
                return 0;
            });
        }
    }
    else if (op == 'I')
    {
        long found = 0;
        eachMemberScore(srcs[0], weights[0], (m, s) {
            double total = s;
            foreach (k; 1 .. srcs.length)
            {
                double other;
                if (!memberScore(srcs[k], m, other))
                    return 0;
                total = aggregate(agg, total, other * weights[k]);
            }
            if (mode == 2)
            {
                found++;
                if (cardLimit && found == cardLimit)
                    return 1;
                return 0;
            }
            acc.zset.add(total, m);
            return 0;
        });
        if (mode == 2)
        {
            repInt(o, found);
            return;
        }
    }
    else // 'D'
    {
        eachMemberScore(srcs[0], 1, (m, s) {
            foreach (k; 1 .. srcs.length)
            {
                double other;
                if (memberScore(srcs[k], m, other))
                    return 0;
            }
            acc.zset.add(s, m);
            return 0;
        });
    }

    if (mode == 2) // ZINTERCARD handled above; union/diff card never reaches here
    {
        repInt(o, cast(long) acc.zset.length);
        return;
    }
    if (mode == 1)
    {
        auto card = acc.zset.length;
        if (card == 0)
        {
            acc.free();
            if (ks.del(dst))
                notifyKeyspaceEvent(NClass.generic, "del", dst);
        }
        else
        {
            ks.d.set(dst, acc); // ownership moves
            notifyKeyspaceEvent(NClass.zset,
                    op == 'U' ? "zunionstore" : (op == 'I' ? "zinterstore" : "zdiffstore"), dst);
        }
        repInt(o, cast(long) card);
        return;
    }
    repArrayHeader(o, acc.zset.length * (withScores ? 2 : 1));
    acc.zset.walkRange(0, acc.zset.length, false, (m, s) {
        repBulk(o, m);
        if (withScores)
            repDouble(o, s);
        return 0;
    });
}
