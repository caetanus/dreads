module dreads.miscops;

// LPOS, LMPOP, SORT, LCS, HRANDFIELD — the remaining list/string/hash tail.

import core.stdc.stdlib : qsort;

import dreads.commands : eqICKeyword, parseDouble, parseLong;
import dreads.dict : StrVal;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;

private void repWrongTypeM(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

/// LPOS key element [RANK rank] [COUNT num] [MAXLEN len]
public void lpos(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'lpos' command");
        return;
    }
    long rank = 1, count = -1, maxlen = 0;
    bool hasCount = false;
    size_t i = 2;
    while (i < args.length)
    {
        if (eqICKeyword(args[i].str, "RANK") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, rank) || rank == 0)
            {
                repError(o,
                        "ERR RANK can't be zero. Use 1 to start searching from the first matching element, or the negative number to start searching from the end.");
                return;
            }
            i += 2;
        }
        else if (eqICKeyword(args[i].str, "COUNT") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, count) || count < 0)
            {
                repError(o, "ERR COUNT can't be negative");
                return;
            }
            hasCount = true;
            i += 2;
        }
        else if (eqICKeyword(args[i].str, "MAXLEN") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, maxlen) || maxlen < 0)
            {
                repError(o, "ERR MAXLEN can't be negative");
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
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
    if (wrong)
    {
        repWrongTypeM(o);
        return;
    }
    auto len = obj is null ? 0 : obj.list.length;
    auto found = arena.allocArray!long(len);
    size_t nfound = 0;
    bool fromTail = rank < 0;
    auto skip = (fromTail ? -rank : rank) - 1;
    long scanned = 0;
    if (obj !is null)
    {
        // walk in the search direction, index reported from the head
        obj.list.walkRange(0, len, (v) { return 0; }); // (kept simple below)
        long idx = fromTail ? cast(long) len - 1 : 0;
        while (idx >= 0 && idx < cast(long) len)
        {
            if (maxlen && scanned == maxlen)
                break;
            bool ok;
            auto v = obj.list.at(idx, ok);
            scanned++;
            if (ok && v == args[1].str)
            {
                if (skip > 0)
                    skip--;
                else
                {
                    found[nfound++] = idx;
                    if (!hasCount || (count != 0 && nfound == cast(size_t) count))
                    {
                        if (!hasCount)
                            break;
                        if (count != 0 && nfound == cast(size_t) count)
                            break;
                    }
                }
            }
            idx += fromTail ? -1 : 1;
        }
    }
    if (!hasCount)
    {
        if (nfound == 0)
            repNullBulk(o);
        else
            repInt(o, found[0]);
        return;
    }
    repArrayHeader(o, nfound);
    foreach (f; found[0 .. nfound])
        repInt(o, f);
}

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
public void lmpop(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
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
    bool fromLeft;
    if (eqICKeyword(rest[0].str, "LEFT"))
        fromLeft = true;
    else if (!eqICKeyword(rest[0].str, "RIGHT"))
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
        auto obj = ks.lookupTyped(k.str, ObjType.list, wrong);
        if (wrong)
        {
            repWrongTypeM(o);
            return;
        }
        if (obj is null || obj.list.length == 0)
            continue;
        auto n = cast(size_t)(count < cast(long) obj.list.length ? count
                : cast(long) obj.list.length);
        repArrayHeader(o, 2);
        repBulk(o, k.str);
        repArrayHeader(o, n);
        foreach (_; 0 .. n)
        {
            repBulk(o, fromLeft ? obj.list.front : obj.list.back);
            if (fromLeft)
                obj.list.popFront();
            else
                obj.list.popBack();
        }
        ks.delIfEmpty(k.str, obj);
        return;
    }
    o.append("*-1\r\n");
}

// ---------------------------------------------------------------------------
// SORT
// ---------------------------------------------------------------------------

private struct SortItem
{
    const(char)[] v;
    double num;
}

extern (C) private int sortNumAsc(scope const void* a, scope const void* b) nothrow @nogc
{
    auto x = cast(const(SortItem)*) a;
    auto y = cast(const(SortItem)*) b;
    return x.num < y.num ? -1 : (x.num > y.num ? 1 : 0);
}

extern (C) private int sortAlphaAsc(scope const void* a, scope const void* b) nothrow @nogc
{
    import core.stdc.string : memcmp;

    auto x = cast(const(SortItem)*) a;
    auto y = cast(const(SortItem)*) b;
    auto minl = x.v.length < y.v.length ? x.v.length : y.v.length;
    if (minl)
    {
        auto c = memcmp(x.v.ptr, y.v.ptr, minl);
        if (c)
            return c < 0 ? -1 : 1;
    }
    return x.v.length < y.v.length ? -1 : (x.v.length > y.v.length ? 1 : 0);
}

/// SORT key [LIMIT off count] [ASC|DESC] [ALPHA] [STORE dst]
/// (BY/GET patterns are not supported — documented in DRIFT.md)
public void sortCmd(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, bool readOnly) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'sort' command");
        return;
    }
    bool alpha, desc;
    long limOff = 0, limCnt = -1;
    const(char)[] storeKey;
    size_t i = 1;
    while (i < args.length)
    {
        auto w = args[i].str;
        if (eqICKeyword(w, "ALPHA"))
        {
            alpha = true;
            i++;
        }
        else if (eqICKeyword(w, "ASC"))
            i++;
        else if (eqICKeyword(w, "DESC"))
        {
            desc = true;
            i++;
        }
        else if (eqICKeyword(w, "LIMIT"))
        {
            if (i + 2 >= args.length || !parseLong(args[i + 1].str, limOff)
                    || !parseLong(args[i + 2].str, limCnt))
            {
                repError(o, "ERR syntax error");
                return;
            }
            i += 3;
        }
        else if (!readOnly && eqICKeyword(w, "STORE") && i + 1 < args.length)
        {
            storeKey = args[i + 1].str;
            i += 2;
        }
        else if (eqICKeyword(w, "BY") || eqICKeyword(w, "GET"))
        {
            repError(o, "ERR BY/GET patterns are not supported");
            return;
        }
        else
        {
            repError(o, "ERR syntax error");
            return;
        }
    }
    auto obj = ks.lookup(args[0].str);
    if (obj !is null && obj.type != ObjType.list && obj.type != ObjType.set
            && obj.type != ObjType.zset)
    {
        repWrongTypeM(o);
        return;
    }
    size_t total = obj is null ? 0 : obj.containerLen;
    auto items = arena.allocArray!SortItem(total);
    size_t n = 0;
    bool numFail = false;
    if (obj !is null)
    {
        int take(const(char)[] v) @nogc nothrow
        {
            items[n].v = v;
            if (!alpha && !parseDouble(v, items[n].num))
                numFail = true;
            n++;
            return 0;
        }

        if (obj.type == ObjType.list)
            obj.list.walkRange(0, total, (v) => take(v));
        else if (obj.type == ObjType.set)
        {
            foreach (si; 0 .. obj.set.capacity)
            {
                if (obj.set.slotLive(si))
                    take(obj.set.keyAt(si));
            }
        }
        else
            obj.zset.walkRange(0, total, false, (m, s) => take(m));
    }
    if (numFail)
    {
        repError(o, "ERR One or more scores can't be converted into double");
        return;
    }
    qsort(items.ptr, n, SortItem.sizeof, alpha ? &sortAlphaAsc : &sortNumAsc);
    if (desc)
    {
        foreach (k; 0 .. n / 2)
        {
            auto t = items[k];
            items[k] = items[n - 1 - k];
            items[n - 1 - k] = t;
        }
    }
    auto hits = items[0 .. n];
    auto off = cast(size_t)(limOff < 0 ? 0 : limOff);
    if (off >= hits.length)
        hits = null;
    else
        hits = hits[off .. $];
    if (limCnt >= 0 && hits.length > cast(size_t) limCnt)
        hits = hits[0 .. cast(size_t) limCnt];

    if (storeKey.length)
    {
        // dst may alias the source: copy values first
        foreach (ref it; hits)
            it.v = arena.dupString(it.v);
        RObj lst;
        lst.type = ObjType.list;
        foreach (ref it; hits)
            lst.list.pushBack(it.v);
        if (hits.length == 0)
            ks.del(storeKey);
        else
            ks.d.set(storeKey, lst);
        repInt(o, cast(long) hits.length);
        return;
    }
    repArrayHeader(o, hits.length);
    foreach (ref it; hits)
        repBulk(o, it.v);
}

// ---------------------------------------------------------------------------
// LCS
// ---------------------------------------------------------------------------

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]
public void lcs(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 2)
    {
        repError(o, "ERR wrong number of arguments for 'lcs' command");
        return;
    }
    bool wantLen, wantIdx, withMatchLen;
    long minMatch = 0;
    size_t i = 2;
    while (i < args.length)
    {
        if (eqICKeyword(args[i].str, "LEN"))
        {
            wantLen = true;
            i++;
        }
        else if (eqICKeyword(args[i].str, "IDX"))
        {
            wantIdx = true;
            i++;
        }
        else if (eqICKeyword(args[i].str, "WITHMATCHLEN"))
        {
            withMatchLen = true;
            i++;
        }
        else if (eqICKeyword(args[i].str, "MINMATCHLEN") && i + 1 < args.length)
        {
            if (!parseLong(args[i + 1].str, minMatch) || minMatch < 0)
            {
                repError(o, "ERR value is not an integer or out of range");
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
    if (wantLen && wantIdx)
    {
        repError(o,
                "ERR If you want both the length and indexes, please just use IDX.");
        return;
    }
    bool w1, w2;
    auto o1 = ks.lookupTyped(args[0].str, ObjType.str, w1);
    auto o2 = ks.lookupTyped(args[1].str, ObjType.str, w2);
    if (w1 || w2)
    {
        repError(o, "ERR The specified keys must contain string values");
        return;
    }
    char[24] sa = void, sbb = void; // scratch for int-encoded operands (both coexist)
    auto a = o1 is null ? "" : o1.str.bytes(sa);
    auto b = o2 is null ? "" : o2.str.bytes(sbb);
    // guard the O(n*m) table
    if (a.length * b.length > 32UL * 1024 * 1024)
    {
        repError(o, "ERR Insufficient memory, strings too long");
        return;
    }
    auto w = b.length + 1;
    auto dp = arena.allocArray!uint((a.length + 1) * w);
    dp[] = 0;
    foreach (ai; 1 .. a.length + 1)
    {
        foreach (bi; 1 .. b.length + 1)
        {
            if (a[ai - 1] == b[bi - 1])
                dp[ai * w + bi] = dp[(ai - 1) * w + (bi - 1)] + 1;
            else
            {
                auto up = dp[(ai - 1) * w + bi];
                auto left = dp[ai * w + (bi - 1)];
                dp[ai * w + bi] = up > left ? up : left;
            }
        }
    }
    auto total = dp[a.length * w + b.length];
    if (wantLen)
    {
        repInt(o, total);
        return;
    }
    if (!wantIdx)
    {
        // backtrack the string itself
        auto outBuf = arena.allocArray!char(total);
        size_t ai = a.length, bi = b.length, pos = total;
        while (ai > 0 && bi > 0)
        {
            if (a[ai - 1] == b[bi - 1])
            {
                outBuf[--pos] = a[ai - 1];
                ai--;
                bi--;
            }
            else if (dp[(ai - 1) * w + bi] >= dp[ai * w + (bi - 1)])
                ai--;
            else
                bi--;
        }
        repBulk(o, outBuf[0 .. total]);
        return;
    }
    // IDX: contiguous match ranges, from the end like Redis
    struct Match
    {
        size_t a1, a2, b1, b2;
    }

    auto matches = arena.allocArray!Match(total ? total : 1);
    size_t nm = 0;
    size_t ai = a.length, bi = b.length;
    size_t runEndA = 0, runEndB = 0, runLen = 0;
    while (ai > 0 && bi > 0)
    {
        if (a[ai - 1] == b[bi - 1])
        {
            if (runLen == 0)
            {
                runEndA = ai - 1;
                runEndB = bi - 1;
            }
            runLen++;
            ai--;
            bi--;
        }
        else
        {
            if (runLen >= 1 && runLen >= cast(size_t) minMatch)
                matches[nm++] = Match(ai, runEndA, bi, runEndB);
            runLen = 0;
            if (dp[(ai - 1) * w + bi] >= dp[ai * w + (bi - 1)])
                ai--;
            else
                bi--;
        }
    }
    if (runLen >= 1 && runLen >= cast(size_t) minMatch)
        matches[nm++] = Match(ai, runEndA, bi, runEndB);

    repArrayHeader(o, 4);
    repBulk(o, "matches");
    repArrayHeader(o, nm);
    foreach (ref m; matches[0 .. nm])
    {
        repArrayHeader(o, withMatchLen ? 3 : 2);
        repArrayHeader(o, 2);
        repInt(o, cast(long) m.a1);
        repInt(o, cast(long) m.a2);
        repArrayHeader(o, 2);
        repInt(o, cast(long) m.b1);
        repInt(o, cast(long) m.b2);
        if (withMatchLen)
            repInt(o, cast(long)(m.a2 - m.a1 + 1));
    }
    repBulk(o, "len");
    repInt(o, total);
}

/// HRANDFIELD key [count [WITHVALUES]] — deterministic (first live slots).
public void hrandfield(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1 || args.length > 3)
    {
        repError(o, "ERR wrong number of arguments for 'hrandfield' command");
        return;
    }
    bool withValues = args.length == 3 && eqICKeyword(args[2].str, "WITHVALUES");
    if (args.length == 3 && !withValues)
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
    auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
    if (wrong)
    {
        repWrongTypeM(o);
        return;
    }
    if (obj is null || obj.hash.length == 0)
    {
        if (withCount)
            repArrayHeader(o, 0);
        else
            repNullBulk(o);
        return;
    }
    if (!withCount)
    {
        foreach (i2; 0 .. obj.hash.capacity)
        {
            if (obj.hash.slotLive(i2))
            {
                repBulk(o, obj.hash.keyAt(i2));
                break;
            }
        }
        return;
    }
    bool repeat = count < 0;
    auto want = cast(size_t)(repeat ? -count : count);
    auto n = repeat ? want : (want < obj.hash.length ? want : obj.hash.length);
    repArrayHeader(o, n * (withValues ? 2 : 1));
    size_t emitted = 0;
    while (emitted < n)
    {
        foreach (i2; 0 .. obj.hash.capacity)
        {
            if (emitted == n)
                break;
            if (!obj.hash.slotLive(i2))
                continue;
            repBulk(o, obj.hash.keyAt(i2));
            if (withValues)
            {
                char[24] sb = void;
                repBulk(o, obj.hash.valAt(i2).bytes(sb));
            }
            emitted++;
        }
    }
}
