module dreads.commands;

// Command dispatch against the typed keyspace. Everything is @nogc: replies
// are written straight into the connection's output ByteBuffer, scratch space
// comes from the per-connection Arena that also holds the parsed command.

import core.checkedint : adds;
import core.stdc.stdio : snprintf;
import core.stdc.stdlib : strtod;
import core.stdc.string : memcpy;

import dreads.dict : Dict, StrVal, Unit;
import dreads.mem : Arena, ByteBuffer, mallocAppend;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;
import dreads.stream : FieldPair, StreamID, nowMs;

/// When a command's effect must reach the AOF (and later the Raft log) in a
/// different form than the client sent it — time- or randomness-dependent
/// commands like EXPIRE (relative -> PEXPIREAT absolute) or XADD * (resolved
/// ID) — its handler leaves the translated RESP command here and the server
/// logs it instead of the raw bytes. Thread-local: the event loop is one
/// thread, and the test runner gets one buffer per thread.
public ByteBuffer propagationOverride;

/// Redis's proto-max-bulk-len default: 512MB per string value.
private enum MAX_STRING_LEN = 512UL * 1024 * 1024;

/// Executes one client command, appending the reply to o.
/// Returns false when the connection should be closed (QUIT).
public bool dispatch(const ref RVal cmd, ref Keyspace ks, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (cmd.type != RType.Array || cmd.arr.length == 0)
    {
        repError(o, "ERR empty command");
        return true;
    }
    foreach (ref a; cmd.arr)
    {
        if (a.type != RType.BulkString && a.type != RType.SimpleString)
        {
            repError(o, "ERR Protocol error: expected bulk string");
            return true;
        }
    }

    auto name = cmd.arr[0].str;
    auto args = cmd.arr[1 .. $];

    char[24] nbuf = void;
    if (name.length > nbuf.length)
    {
        unknownCmd(o, name);
        return true;
    }
    foreach (i, c; name)
        nbuf[i] = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;

    switch (cast(string) nbuf[0 .. name.length])
    {
        // --- connection / server ---
    case "PING":
        {
            if (args.length == 0)
                repSimple(o, "PONG");
            else if (args.length == 1)
                repBulk(o, args[0].str);
            else
                arityErr(o, "ping");
            break;
        }
    case "ECHO":
        {
            if (args.length == 1)
                repBulk(o, args[0].str);
            else
                arityErr(o, "echo");
            break;
        }
    case "COMMAND":
        {
            repArrayHeader(o, 0); // stub so redis-cli's handshake succeeds
            break;
        }
    case "QUIT":
        {
            repSimple(o, "OK");
            return false;
        }

        // --- keyspace ---
    case "DEL":
    case "UNLINK": // same effect; our free is synchronous either way
        {
            if (args.length == 0)
            {
                arityErr(o, name.length == 3 ? "del" : "unlink");
                break;
            }
            long n = 0;
            foreach (ref a; args)
                n += ks.del(a.str) ? 1 : 0;
            repInt(o, n);
            break;
        }
    case "EXISTS":
        {
            if (args.length == 0)
            {
                arityErr(o, "exists");
                break;
            }
            long n = 0;
            foreach (ref a; args)
                n += ks.exists(a.str) ? 1 : 0;
            repInt(o, n);
            break;
        }
    case "TYPE":
        {
            if (args.length != 1)
            {
                arityErr(o, "type");
                break;
            }
            auto obj = ks.lookup(args[0].str);
            repSimple(o, obj is null ? "none" : obj.typeName);
            break;
        }
    case "KEYS":
        {
            if (args.length != 1)
            {
                arityErr(o, "keys");
                break;
            }
            auto pat = args[0].str;
            size_t n = 0;
            foreach (k, ref v; ks)
            {
                if (globMatch(pat, k))
                    n++;
            }
            repArrayHeader(o, n);
            foreach (k, ref v; ks)
            {
                if (globMatch(pat, k))
                    repBulk(o, k);
            }
            break;
        }
    case "DBSIZE":
        {
            if (args.length == 0)
                repInt(o, cast(long) ks.length);
            else
                arityErr(o, "dbsize");
            break;
        }
    case "FLUSHALL":
    case "FLUSHDB": // single database: same thing
        {
            ks.clear();
            repSimple(o, "OK");
            break;
        }
    case "EXPIRE":
    case "PEXPIRE":
    case "EXPIREAT":
    case "PEXPIREAT":
        {
            // EXPIRE(6)/EXPIREAT(8) take seconds; EXPIRE(6)/PEXPIRE(7) are relative
            bool isSec = name.length == 6 || name.length == 8;
            bool isRel = name.length <= 7;
            if (args.length != 2)
            {
                arityErr(o, isRel ? (isSec ? "expire" : "pexpire") : (isSec
                        ? "expireat" : "pexpireat"));
                break;
            }
            long v;
            if (!parseLong(args[1].str, v))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            auto obj = ks.lookup(args[0].str);
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            long absMs;
            if (!resolveExpireMs(v, isSec, isRel, absMs))
            {
                repError(o, "ERR invalid expire time");
                break;
            }
            obj.expireAtMs = absMs <= 0 ? 1 : cast(ulong) absMs;
            propagatePexpireat(args[0].str, obj.expireAtMs);
            repInt(o, 1);
            break;
        }
    case "TTL":
    case "PTTL":
        {
            bool inSec = name.length == 3;
            if (args.length != 1)
            {
                arityErr(o, inSec ? "ttl" : "pttl");
                break;
            }
            auto obj = ks.lookup(args[0].str);
            if (obj is null)
            {
                repInt(o, -2);
                break;
            }
            if (obj.expireAtMs == 0)
            {
                repInt(o, -1);
                break;
            }
            long rem = cast(long)(obj.expireAtMs - nowMs());
            if (rem < 0)
                rem = 0;
            repInt(o, inSec ? (rem + 500) / 1000 : rem);
            break;
        }
    case "PERSIST":
        {
            if (args.length != 1)
            {
                arityErr(o, "persist");
                break;
            }
            auto obj = ks.lookup(args[0].str);
            if (obj !is null && obj.expireAtMs != 0)
            {
                obj.expireAtMs = 0;
                repInt(o, 1);
            }
            else
                repInt(o, 0);
            break;
        }

        // --- strings ---
    case "SET":
        {
            if (args.length < 2)
            {
                arityErr(o, "set");
                break;
            }
            long absExpire = -1; // resolved absolute ms; -1 = leave untouched
            bool nx, xx, wantGet, keepttl, badSyntax, badExpire;
            size_t i = 2;
            while (i < args.length)
            {
                auto opt = args[i].str;
                if (eqICKeyword(opt, "NX"))
                {
                    nx = true;
                    i++;
                }
                else if (eqICKeyword(opt, "XX"))
                {
                    xx = true;
                    i++;
                }
                else if (eqICKeyword(opt, "GET"))
                {
                    wantGet = true;
                    i++;
                }
                else if (eqICKeyword(opt, "KEEPTTL"))
                {
                    keepttl = true;
                    i++;
                }
                else if (eqICKeyword(opt, "EX") || eqICKeyword(opt, "PX")
                        || eqICKeyword(opt, "EXAT") || eqICKeyword(opt, "PXAT"))
                {
                    long v;
                    if (i + 1 >= args.length || !parseLong(args[i + 1].str, v))
                    {
                        badSyntax = true;
                        break;
                    }
                    bool isRel = opt.length == 2;
                    bool isSec = eqICKeyword(opt, "EX") || eqICKeyword(opt, "EXAT");
                    if ((isRel && v <= 0) || !resolveExpireMs(v, isSec, isRel, absExpire))
                    {
                        badExpire = true;
                        break;
                    }
                    i += 2;
                }
                else
                {
                    badSyntax = true;
                    break;
                }
            }
            if (badExpire)
            {
                repError(o, "ERR invalid expire time in 'set' command");
                break;
            }
            if (badSyntax || (nx && xx) || (keepttl && absExpire >= 0))
            {
                repError(o, "ERR syntax error");
                break;
            }
            auto existing = ks.lookup(args[0].str);
            if (wantGet && existing !is null && existing.type != ObjType.str)
            {
                repWrongType(o);
                break;
            }
            if ((nx && existing !is null) || (xx && existing is null))
            {
                if (wantGet && existing !is null)
                    repBulk(o, existing.str.s);
                else
                    repNullBulk(o);
                break;
            }
            if (wantGet)
            {
                if (existing !is null)
                    repBulk(o, existing.str.s);
                else
                    repNullBulk(o);
            }
            else
                repSimple(o, "OK");
            ulong kept = keepttl && existing !is null ? existing.expireAtMs : 0;
            ks.setStr(args[0].str, args[1].str);
            auto obj = ks.lookup(args[0].str);
            if (absExpire >= 0)
                obj.expireAtMs = absExpire == 0 ? 1 : cast(ulong) absExpire;
            else if (keepttl)
                obj.expireAtMs = kept;
            if (args.length > 2)
                propagateSet(args[0].str, args[1].str, obj.expireAtMs);
            break;
        }
    case "SETEX":
    case "PSETEX":
        {
            bool isSec = name.length == 5;
            if (args.length != 3)
            {
                arityErr(o, isSec ? "setex" : "psetex");
                break;
            }
            long v;
            if (!parseLong(args[1].str, v))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            long absMs;
            if (v <= 0 || !resolveExpireMs(v, isSec, true, absMs))
            {
                repError(o, isSec ? "ERR invalid expire time in 'setex' command"
                        : "ERR invalid expire time in 'psetex' command");
                break;
            }
            ks.setStr(args[0].str, args[2].str);
            auto obj = ks.lookup(args[0].str);
            obj.expireAtMs = cast(ulong) absMs;
            propagateSet(args[0].str, args[2].str, obj.expireAtMs);
            repSimple(o, "OK");
            break;
        }
    case "GETEX":
        {
            if (args.length == 0)
            {
                arityErr(o, "getex");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long absMs = -1;
            bool doPersist;
            if (args.length == 2 && eqICKeyword(args[1].str, "PERSIST"))
                doPersist = true;
            else if (args.length == 3)
            {
                auto opt = args[1].str;
                long v;
                bool isRel = opt.length == 2;
                bool isSec = eqICKeyword(opt, "EX") || eqICKeyword(opt, "EXAT");
                if (!(eqICKeyword(opt, "EX") || eqICKeyword(opt, "PX")
                        || eqICKeyword(opt, "EXAT") || eqICKeyword(opt, "PXAT"))
                        || !parseLong(args[2].str, v) || (isRel && v <= 0)
                        || !resolveExpireMs(v, isSec, isRel, absMs))
                {
                    repError(o, "ERR syntax error");
                    break;
                }
            }
            else if (args.length != 1)
            {
                repError(o, "ERR syntax error");
                break;
            }
            if (obj is null)
            {
                repNullBulk(o);
                break;
            }
            repBulk(o, obj.str.s);
            if (doPersist && obj.expireAtMs != 0)
            {
                obj.expireAtMs = 0;
                propagatePersist(args[0].str);
            }
            else if (absMs >= 0)
            {
                obj.expireAtMs = absMs == 0 ? 1 : cast(ulong) absMs;
                propagatePexpireat(args[0].str, obj.expireAtMs);
            }
            break;
        }
    case "GETDEL":
        {
            if (args.length != 1)
            {
                arityErr(o, "getdel");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repNullBulk(o);
                break;
            }
            repBulk(o, obj.str.s);
            ks.del(args[0].str);
            break;
        }
    case "SETNX":
        {
            if (args.length != 2)
            {
                arityErr(o, "setnx");
                break;
            }
            if (ks.exists(args[0].str))
                repInt(o, 0);
            else
            {
                ks.setStr(args[0].str, args[1].str);
                repInt(o, 1);
            }
            break;
        }
    case "GET":
        {
            if (args.length != 1)
            {
                arityErr(o, "get");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
                repWrongType(o);
            else if (obj is null)
                repNullBulk(o);
            else
                repBulk(o, obj.str.s);
            break;
        }
    case "GETSET":
        {
            if (args.length != 2)
            {
                arityErr(o, "getset");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
                repNullBulk(o);
            else
                repBulk(o, obj.str.s);
            ks.setStr(args[0].str, args[1].str);
            break;
        }
    case "APPEND":
        {
            if (args.length != 2)
            {
                arityErr(o, "append");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj !is null && obj.str.s.length + args[1].str.length > MAX_STRING_LEN)
            {
                repError(o, "ERR string exceeds maximum allowed size (proto-max-bulk-len)");
                break;
            }
            if (obj is null)
            {
                ks.setStr(args[0].str, args[1].str);
                repInt(o, cast(long) args[1].str.length);
            }
            else
            {
                obj.str.s = mallocAppend(obj.str.s, args[1].str);
                repInt(o, cast(long) obj.str.s.length);
            }
            break;
        }
    case "STRLEN":
        {
            if (args.length != 1)
            {
                arityErr(o, "strlen");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.str.s.length);
            break;
        }
    case "INCR":
        {
            if (args.length != 1)
                arityErr(o, "incr");
            else
                incrDecr(ks, args[0].str, 1, o);
            break;
        }
    case "DECR":
        {
            if (args.length != 1)
                arityErr(o, "decr");
            else
                incrDecr(ks, args[0].str, -1, o);
            break;
        }
    case "INCRBY":
    case "DECRBY":
        {
            if (args.length != 2)
            {
                arityErr(o, nbuf[0] == 'I' ? "incrby" : "decrby");
                break;
            }
            long delta;
            if (!parseLong(args[1].str, delta))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            incrDecr(ks, args[0].str, nbuf[0] == 'I' ? delta : -delta, o);
            break;
        }
    case "MSET":
        {
            if (args.length == 0 || args.length % 2 != 0)
            {
                arityErr(o, "mset");
                break;
            }
            for (size_t i = 0; i < args.length; i += 2)
                ks.setStr(args[i].str, args[i + 1].str);
            repSimple(o, "OK");
            break;
        }
    case "MGET":
        {
            if (args.length == 0)
            {
                arityErr(o, "mget");
                break;
            }
            repArrayHeader(o, args.length);
            foreach (ref a; args)
            {
                auto obj = ks.lookup(a.str);
                if (obj !is null && obj.type == ObjType.str)
                    repBulk(o, obj.str.s);
                else
                    repNullBulk(o); // missing or wrong type both read as nil
            }
            break;
        }

        // --- lists ---
    case "LPUSH":
    case "RPUSH":
        {
            if (args.length < 2)
            {
                arityErr(o, nbuf[0] == 'L' ? "lpush" : "rpush");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            foreach (ref a; args[1 .. $])
            {
                if (nbuf[0] == 'L')
                    obj.list.pushFront(a.str);
                else
                    obj.list.pushBack(a.str);
            }
            repInt(o, cast(long) obj.list.length);
            break;
        }
    case "LPOP":
    case "RPOP":
        {
            listPop(ks, args, nbuf[0] == 'L', o);
            break;
        }
    case "LLEN":
        {
            if (args.length != 1)
            {
                arityErr(o, "llen");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.list.length);
            break;
        }
    case "LRANGE":
        {
            if (args.length != 3)
            {
                arityErr(o, "lrange");
                break;
            }
            long start, stop;
            if (!parseLong(args[1].str, start) || !parseLong(args[2].str, stop))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto len = obj is null ? 0 : cast(long) obj.list.length;
            normalizeRange(start, stop, len);
            if (start > stop)
            {
                repArrayHeader(o, 0);
                break;
            }
            repArrayHeader(o, cast(size_t)(stop - start + 1));
            obj.list.walkRange(start, cast(size_t)(stop - start + 1), (v) {
                repBulk(o, v);
                return 0;
            });
            break;
        }
    case "LINDEX":
        {
            if (args.length != 2)
            {
                arityErr(o, "lindex");
                break;
            }
            long idx;
            if (!parseLong(args[1].str, idx))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            bool ok;
            auto v = obj is null ? null : obj.list.at(idx, ok);
            if (ok)
                repBulk(o, v);
            else
                repNullBulk(o);
            break;
        }
    case "LSET":
        {
            if (args.length != 3)
            {
                arityErr(o, "lset");
                break;
            }
            long idx;
            if (!parseLong(args[1].str, idx))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
                repWrongType(o);
            else if (obj is null)
                repError(o, "ERR no such key");
            else if (obj.list.setAt(idx, args[2].str))
                repSimple(o, "OK");
            else
                repError(o, "ERR index out of range");
            break;
        }
    case "LREM":
        {
            if (args.length != 3)
            {
                arityErr(o, "lrem");
                break;
            }
            long rcount;
            if (!parseLong(args[1].str, rcount))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            auto removed = obj.list.remove(rcount, args[2].str);
            ks.delIfEmpty(args[0].str, obj);
            repInt(o, removed);
            break;
        }

        // --- hashes ---
    case "HSET":
        {
            if (args.length < 3 || (args.length - 1) % 2 != 0)
            {
                arityErr(o, "hset");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long added = 0;
            for (size_t i = 1; i < args.length; i += 2)
                added += obj.hash.set(args[i].str, StrVal.of(args[i + 1].str)) ? 1 : 0;
            repInt(o, added);
            break;
        }
    case "HGET":
        {
            if (args.length != 2)
            {
                arityErr(o, "hget");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto f = obj is null ? null : obj.hash.get(args[1].str);
            if (f is null)
                repNullBulk(o);
            else
                repBulk(o, f.s);
            break;
        }
    case "HMGET":
        {
            if (args.length < 2)
            {
                arityErr(o, "hmget");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            repArrayHeader(o, args.length - 1);
            foreach (ref a; args[1 .. $])
            {
                auto f = obj is null ? null : obj.hash.get(a.str);
                if (f is null)
                    repNullBulk(o);
                else
                    repBulk(o, f.s);
            }
            break;
        }
    case "HDEL":
        {
            if (args.length < 2)
            {
                arityErr(o, "hdel");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            long n = 0;
            foreach (ref a; args[1 .. $])
                n += obj.hash.del(a.str) ? 1 : 0;
            ks.delIfEmpty(args[0].str, obj);
            repInt(o, n);
            break;
        }
    case "HLEN":
        {
            if (args.length != 1)
            {
                arityErr(o, "hlen");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.hash.length);
            break;
        }
    case "HEXISTS":
        {
            if (args.length != 2)
            {
                arityErr(o, "hexists");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj !is null && obj.hash.exists(args[1].str) ? 1 : 0);
            break;
        }
    case "HKEYS":
    case "HVALS":
    case "HGETALL":
        {
            if (args.length != 1)
            {
                arityErr(o, nbuf[1] == 'K' ? "hkeys" : (nbuf[1] == 'V' ? "hvals" : "hgetall"));
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto len = obj is null ? 0 : obj.hash.length;
            bool wantKeys = nbuf[1] == 'K' || nbuf[1] == 'G';
            bool wantVals = nbuf[1] == 'V' || nbuf[1] == 'G';
            repArrayHeader(o, len * (wantKeys && wantVals ? 2 : 1));
            if (obj !is null)
            {
                foreach (k, ref v; obj.hash)
                {
                    if (wantKeys)
                        repBulk(o, k);
                    if (wantVals)
                        repBulk(o, v.s);
                }
            }
            break;
        }
    case "HINCRBY":
        {
            if (args.length != 3)
            {
                arityErr(o, "hincrby");
                break;
            }
            long delta;
            if (!parseLong(args[2].str, delta))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long cur = 0;
            auto f = obj.hash.get(args[1].str);
            if (f !is null && !parseLong(f.s, cur))
            {
                repError(o, "ERR hash value is not an integer");
                break;
            }
            bool ovf;
            auto nv = adds(cur, delta, ovf);
            if (ovf)
            {
                repError(o, "ERR increment or decrement would overflow");
                break;
            }
            char[24] buf = void;
            auto blen = snprintf(buf.ptr, buf.length, "%lld", nv);
            obj.hash.set(args[1].str, StrVal.of(buf[0 .. blen]));
            repInt(o, nv);
            break;
        }

        // --- sets ---
    case "SADD":
        {
            if (args.length < 2)
            {
                arityErr(o, "sadd");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long n = 0;
            foreach (ref a; args[1 .. $])
                n += obj.set.set(a.str, Unit()) ? 1 : 0;
            repInt(o, n);
            break;
        }
    case "SREM":
        {
            if (args.length < 2)
            {
                arityErr(o, "srem");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            long n = 0;
            foreach (ref a; args[1 .. $])
                n += obj.set.del(a.str) ? 1 : 0;
            ks.delIfEmpty(args[0].str, obj);
            repInt(o, n);
            break;
        }
    case "SISMEMBER":
        {
            if (args.length != 2)
            {
                arityErr(o, "sismember");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj !is null && obj.set.exists(args[1].str) ? 1 : 0);
            break;
        }
    case "SCARD":
        {
            if (args.length != 1)
            {
                arityErr(o, "scard");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.set.length);
            break;
        }
    case "SMEMBERS":
        {
            if (args.length != 1)
            {
                arityErr(o, "smembers");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            repArrayHeader(o, obj is null ? 0 : obj.set.length);
            if (obj !is null)
            {
                foreach (m, ref u; obj.set)
                    repBulk(o, m);
            }
            break;
        }
    case "SINTER":
    case "SDIFF":
        {
            setCombine(ks, args, nbuf[1] == 'I', o, arena);
            break;
        }
    case "SUNION":
        {
            setUnion(ks, args, o);
            break;
        }

        // --- sorted sets ---
    case "ZADD":
        {
            if (args.length < 3 || (args.length - 1) % 2 != 0)
            {
                arityErr(o, "zadd");
                break;
            }
            // validate every score before touching the keyspace
            for (size_t i = 1; i < args.length; i += 2)
            {
                double s;
                if (!parseDouble(args[i].str, s))
                {
                    repError(o, "ERR value is not a valid float");
                    return true;
                }
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long added = 0;
            for (size_t i = 1; i < args.length; i += 2)
            {
                double s;
                parseDouble(args[i].str, s);
                added += obj.zset.add(s, args[i + 1].str) ? 1 : 0;
            }
            repInt(o, added);
            break;
        }
    case "ZREM":
        {
            if (args.length < 2)
            {
                arityErr(o, "zrem");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            long n = 0;
            foreach (ref a; args[1 .. $])
                n += obj.zset.remove(a.str) ? 1 : 0;
            ks.delIfEmpty(args[0].str, obj);
            repInt(o, n);
            break;
        }
    case "ZSCORE":
        {
            if (args.length != 2)
            {
                arityErr(o, "zscore");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            double s;
            if (obj !is null && obj.zset.score(args[1].str, s))
                repDouble(o, s);
            else
                repNullBulk(o);
            break;
        }
    case "ZINCRBY":
        {
            if (args.length != 3)
            {
                arityErr(o, "zincrby");
                break;
            }
            double delta;
            if (!parseDouble(args[1].str, delta))
            {
                repError(o, "ERR value is not a valid float");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            double cur = 0;
            obj.zset.score(args[2].str, cur);
            obj.zset.add(cur + delta, args[2].str);
            repDouble(o, cur + delta);
            break;
        }
    case "ZCARD":
        {
            if (args.length != 1)
            {
                arityErr(o, "zcard");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.zset.length);
            break;
        }
    case "ZRANK":
    case "ZREVRANK":
        {
            bool rev = name.length != 5;
            if (args.length != 2)
            {
                arityErr(o, rev ? "zrevrank" : "zrank");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            bool ok;
            auto r = obj is null ? 0 : obj.zset.rank(args[1].str, ok);
            if (!ok)
                repNullBulk(o);
            else
                repInt(o, rev ? cast(long)(obj.zset.length - 1 - r) : cast(long) r);
            break;
        }
    case "ZRANGE":
    case "ZREVRANGE":
        {
            import dreads.zsetops : zrangeGeneric;

            zrangeGeneric(ks, args, o, arena, name.length == 6 ? 0 : 1);
            break;
        }
    case "ZRANGEBYSCORE":
    case "ZREVRANGEBYSCORE":
        {
            import dreads.zsetops : zrangeGeneric;

            zrangeGeneric(ks, args, o, arena, name.length == 13 ? 2 : 3);
            break;
        }
    case "ZRANGEBYLEX":
    case "ZREVRANGEBYLEX":
        {
            import dreads.zsetops : zrangeGeneric;

            zrangeGeneric(ks, args, o, arena, name.length == 11 ? 4 : 5);
            break;
        }
    case "ZRANGESTORE":
        {
            import dreads.zsetops : zrangestore;

            zrangestore(ks, args, o, arena);
            break;
        }
    case "ZLEXCOUNT":
    case "ZREMRANGEBYLEX":
        {
            import dreads.zsetops : zlexRange;

            zlexRange(ks, args, o, arena, name.length == 14);
            break;
        }
    case "ZRANDMEMBER":
        {
            import dreads.zsetops : zrandmember;

            zrandmember(ks, args, o);
            break;
        }
    case "ZMPOP":
        {
            import dreads.zsetops : zmpop;

            zmpop(ks, args, o, arena);
            break;
        }
    case "ZUNION":
    case "ZINTER":
    case "ZDIFF":
        {
            import dreads.zsetops : zsetCombine;

            zsetCombine(ks, args, o, arena, nbuf[1] == 'U' ? 'U' : (nbuf[1] == 'I' ? 'I' : 'D'), 0);
            break;
        }
    case "ZUNIONSTORE":
    case "ZINTERSTORE":
    case "ZDIFFSTORE":
        {
            import dreads.zsetops : zsetCombine;

            zsetCombine(ks, args, o, arena, nbuf[1] == 'U' ? 'U' : (nbuf[1] == 'I' ? 'I' : 'D'), 1);
            break;
        }
    case "ZINTERCARD":
        {
            import dreads.zsetops : zsetCombine;

            zsetCombine(ks, args, o, arena, 'I', 2);
            break;
        }

        // --- keyspace extras / server ---
    case "RENAME":
        {
            if (args.length != 2)
            {
                arityErr(o, "rename");
                break;
            }
            if (ks.rename(args[0].str, args[1].str))
                repSimple(o, "OK");
            else
                repError(o, "ERR no such key");
            break;
        }
    case "RENAMENX":
        {
            if (args.length != 2)
            {
                arityErr(o, "renamenx");
                break;
            }
            if (!ks.exists(args[0].str))
            {
                repError(o, "ERR no such key");
                break;
            }
            if (ks.exists(args[1].str))
            {
                repInt(o, 0);
                break;
            }
            ks.rename(args[0].str, args[1].str);
            repInt(o, 1);
            break;
        }
    case "TIME":
        {
            auto ms = nowMs();
            char[24] b = void;
            repArrayHeader(o, 2);
            auto n = snprintf(b.ptr, b.length, "%llu", ms / 1000);
            repBulk(o, b[0 .. n]);
            n = snprintf(b.ptr, b.length, "%llu", (ms % 1000) * 1000);
            repBulk(o, b[0 .. n]);
            break;
        }
    case "SELECT":
        {
            if (args.length == 1 && args[0].str == "0")
                repSimple(o, "OK"); // single database
            else
                repError(o, "ERR DB index is out of range");
            break;
        }
    case "CONFIG":
        {
            if (args.length >= 1 && eqICKeyword(args[0].str, "GET"))
                repArrayHeader(o, 0); // stub: no configurable parameters yet
            else if (args.length >= 1 && eqICKeyword(args[0].str, "SET"))
                repSimple(o, "OK");
            else
                repError(o, "ERR Unknown CONFIG subcommand");
            break;
        }
    case "INFO":
        {
            char[160] b = void;
            auto n = snprintf(b.ptr, b.length,
                    "# Server\r\nredis_version:7.4.0\r\nserver_name:dreads\r\n# Keyspace\r\ndb0:keys=%zu,expires=0\r\n",
                    ks.length);
            repBulk(o, b[0 .. n]);
            break;
        }

        // --- string extras ---
    case "GETRANGE":
        {
            if (args.length != 3)
            {
                arityErr(o, "getrange");
                break;
            }
            long start, stop;
            if (!parseLong(args[1].str, start) || !parseLong(args[2].str, stop))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto len = obj is null ? 0 : cast(long) obj.str.s.length;
            normalizeRange(start, stop, len);
            if (len == 0 || start > stop)
                repBulk(o, "");
            else
                repBulk(o, obj.str.s[cast(size_t) start .. cast(size_t) stop + 1]);
            break;
        }
    case "SETRANGE":
        {
            if (args.length != 3)
            {
                arityErr(o, "setrange");
                break;
            }
            long off;
            if (!parseLong(args[1].str, off) || off < 0)
            {
                repError(o, "ERR offset is out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto v = args[2].str;
            auto oldLen = obj is null ? 0 : obj.str.s.length;
            if (v.length == 0)
            {
                repInt(o, cast(long) oldLen);
                break;
            }
            auto newLen = cast(size_t) off + v.length > oldLen ? cast(size_t) off + v.length
                : oldLen;
            if (newLen > MAX_STRING_LEN)
            {
                repError(o, "ERR string exceeds maximum allowed size (proto-max-bulk-len)");
                break;
            }
            auto buf = arena.allocArray!char(newLen);
            buf[] = '\0';
            if (oldLen)
                buf[0 .. oldLen] = obj.str.s;
            buf[cast(size_t) off .. cast(size_t) off + v.length] = v;
            if (obj is null)
                ks.setStr(args[0].str, buf);
            else
            {
                import dreads.mem : freeSlice, mallocDup;

                obj.str.s.freeSlice;
                obj.str.s = buf.mallocDup;
            }
            repInt(o, cast(long) newLen);
            break;
        }
    case "INCRBYFLOAT":
        {
            if (args.length != 2)
            {
                arityErr(o, "incrbyfloat");
                break;
            }
            double delta;
            if (!parseDouble(args[1].str, delta))
            {
                repError(o, "ERR value is not a valid float");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.str, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            double cur = 0;
            if (obj !is null && !parseDouble(obj.str.s, cur))
            {
                repError(o, "ERR value is not a valid float");
                break;
            }
            auto nv = cur + delta;
            if (nv != nv || nv == double.infinity || nv == -double.infinity)
            {
                repError(o, "ERR increment would produce NaN or Infinity");
                break;
            }
            char[40] b = void;
            auto res = fmtDouble(b, nv);
            ulong keptTtl = obj is null ? 0 : obj.expireAtMs;
            if (obj is null)
                ks.setStr(args[0].str, res);
            else
            {
                import dreads.mem : freeSlice, mallocDup;

                obj.str.s.freeSlice;
                obj.str.s = res.mallocDup;
            }
            repBulk(o, res);
            // float math is logged as its result, never re-derived
            propagateSet(args[0].str, res, keptTtl);
            break;
        }
    case "MSETNX":
        {
            if (args.length == 0 || args.length % 2 != 0)
            {
                arityErr(o, "msetnx");
                break;
            }
            bool any;
            for (size_t i = 0; i < args.length; i += 2)
                any = any || ks.exists(args[i].str);
            if (any)
            {
                repInt(o, 0);
                break;
            }
            for (size_t i = 0; i < args.length; i += 2)
                ks.setStr(args[i].str, args[i + 1].str);
            repInt(o, 1);
            break;
        }

        // --- list extras ---
    case "LTRIM":
        {
            if (args.length != 3)
            {
                arityErr(o, "ltrim");
                break;
            }
            long start, stop;
            if (!parseLong(args[1].str, start) || !parseLong(args[2].str, stop))
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repSimple(o, "OK");
                break;
            }
            auto len = cast(long) obj.list.length;
            normalizeRange(start, stop, len);
            if (start > stop)
                ks.del(args[0].str);
            else
            {
                foreach (_; 0 .. start)
                    obj.list.popFront();
                foreach (_; 0 .. len - 1 - stop)
                    obj.list.popBack();
            }
            repSimple(o, "OK");
            break;
        }
    case "LINSERT":
        {
            if (args.length != 4)
            {
                arityErr(o, "linsert");
                break;
            }
            bool before;
            if (eqICKeyword(args[1].str, "BEFORE"))
                before = true;
            else if (!eqICKeyword(args[1].str, "AFTER"))
            {
                repError(o, "ERR syntax error");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            repInt(o, obj.list.insertAround(args[2].str, args[3].str, before));
            break;
        }
    case "LMOVE":
        {
            if (args.length != 4)
            {
                arityErr(o, "lmove");
                break;
            }
            bool fromLeft, toLeft;
            if (!parseSide(args[2].str, fromLeft) || !parseSide(args[3].str, toLeft))
            {
                repError(o, "ERR syntax error");
                break;
            }
            lmove(ks, args[0].str, args[1].str, fromLeft, toLeft, o, arena);
            break;
        }
    case "RPOPLPUSH":
        {
            if (args.length != 2)
            {
                arityErr(o, "rpoplpush");
                break;
            }
            lmove(ks, args[0].str, args[1].str, false, true, o, arena);
            break;
        }

        // --- hash extras ---
    case "HSETNX":
        {
            if (args.length != 3)
            {
                arityErr(o, "hsetnx");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj.hash.exists(args[1].str))
                repInt(o, 0);
            else
            {
                obj.hash.set(args[1].str, StrVal.of(args[2].str));
                repInt(o, 1);
            }
            break;
        }
    case "HSTRLEN":
        {
            if (args.length != 2)
            {
                arityErr(o, "hstrlen");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto f = obj is null ? null : obj.hash.get(args[1].str);
            repInt(o, f is null ? 0 : cast(long) f.s.length);
            break;
        }

        // --- set extras ---
    case "SPOP":
        {
            spop(ks, args, o, arena);
            break;
        }
    case "SRANDMEMBER":
        {
            srandmember(ks, args, o);
            break;
        }
    case "SMOVE":
        {
            if (args.length != 3)
            {
                arityErr(o, "smove");
                break;
            }
            bool w1, w2;
            auto src = ks.lookupTyped(args[0].str, ObjType.set, w1);
            ks.lookupTyped(args[1].str, ObjType.set, w2);
            if (w1 || w2)
            {
                repWrongType(o);
                break;
            }
            if (src is null || !src.set.exists(args[2].str))
            {
                repInt(o, 0);
                break;
            }
            src.set.del(args[2].str);
            bool w3;
            auto dst = ks.getOrCreate(args[1].str, ObjType.set, w3); // may rehash: src is stale now
            dst.set.set(args[2].str, Unit());
            auto src2 = ks.lookup(args[0].str);
            if (src2 !is null)
                ks.delIfEmpty(args[0].str, src2);
            repInt(o, 1);
            break;
        }
    case "SMISMEMBER":
        {
            if (args.length < 2)
            {
                arityErr(o, "smismember");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            repArrayHeader(o, args.length - 1);
            foreach (ref a; args[1 .. $])
                repInt(o, obj !is null && obj.set.exists(a.str) ? 1 : 0);
            break;
        }
    case "SINTERCARD":
        {
            sintercard(ks, args, o, arena);
            break;
        }
    case "SINTERSTORE":
    case "SUNIONSTORE":
    case "SDIFFSTORE":
        {
            // nbuf[1]: I(nter) / U(nion) / D(iff)
            setStore(ks, args, nbuf[1], o, arena);
            break;
        }

        // --- zset extras ---
    case "ZCOUNT":
        {
            if (args.length != 3)
            {
                arityErr(o, "zcount");
                break;
            }
            double min, max;
            bool minExcl, maxExcl;
            if (!parseScoreBound(args[1].str, min, minExcl) || !parseScoreBound(args[2].str,
                    max, maxExcl))
            {
                repError(o, "ERR min or max is not a float");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long n = 0;
            if (obj !is null)
                obj.zset.walkScoreRange(min, minExcl, max, maxExcl, (m, s) {
                    n++;
                    return 0;
                });
            repInt(o, n);
            break;
        }
    case "ZPOPMIN":
    case "ZPOPMAX":
        {
            zpop(ks, args, nbuf[5] == 'A', o, arena); // ZPOPM[A]X vs ZPOPM[I]N
            break;
        }
    case "ZMSCORE":
        {
            if (args.length < 2)
            {
                arityErr(o, "zmscore");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            repArrayHeader(o, args.length - 1);
            foreach (ref a; args[1 .. $])
            {
                double s;
                if (obj !is null && obj.zset.score(a.str, s))
                    repDouble(o, s);
                else
                    repNullBulk(o);
            }
            break;
        }
    case "ZREMRANGEBYRANK":
    case "ZREMRANGEBYSCORE":
        {
            zremrange(ks, args, name.length == 15, o, arena); // BYRANK is 15 chars
            break;
        }

        // --- streams ---
    case "XADD":
        {
            if (args.length < 4 || (args.length - 2) % 2 != 0)
            {
                arityErr(o, "xadd");
                break;
            }
            bool autoId = args[1].str == "*";
            StreamID id;
            if (!autoId)
            {
                if (!parseStreamId(args[1].str, 0, id))
                {
                    repError(o, "ERR Invalid stream ID specified as stream command argument");
                    break;
                }
                if (id == StreamID(0, 0))
                {
                    repError(o, "ERR The ID specified in XADD must be greater than 0-0");
                    break;
                }
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.stream, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (autoId)
                id = obj.stream.nextId(nowMs());
            auto np = (args.length - 2) / 2;
            auto pairs = arena.allocArray!FieldPair(np);
            foreach (i; 0 .. np)
            {
                pairs[i].field = args[2 + i * 2].str;
                pairs[i].value = args[3 + i * 2].str;
            }
            if (!obj.stream.add(id, pairs))
            {
                repError(o,
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
                break;
            }
            repStreamId(o, id);
            if (autoId)
            {
                // the log must carry the resolved ID, never "*"
                propagationOverride.clear();
                repArrayHeader(propagationOverride, args.length + 1);
                repBulk(propagationOverride, "XADD");
                repBulk(propagationOverride, args[0].str);
                char[48] b = void;
                auto blen = snprintf(b.ptr, b.length, "%llu-%llu", id.ms, id.seq);
                repBulk(propagationOverride, b[0 .. blen]);
                foreach (ref a; args[2 .. $])
                    repBulk(propagationOverride, a.str);
            }
            break;
        }
    case "XLEN":
        {
            if (args.length != 1)
            {
                arityErr(o, "xlen");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
            if (wrong)
                repWrongType(o);
            else
                repInt(o, obj is null ? 0 : cast(long) obj.stream.length);
            break;
        }
    case "XRANGE":
        {
            size_t limit = 0;
            if (args.length == 5 && eqICKeyword(args[3].str, "COUNT"))
            {
                long n;
                if (!parseLong(args[4].str, n) || n < 0)
                {
                    repError(o, "ERR value is not an integer or out of range");
                    break;
                }
                limit = cast(size_t) n;
            }
            else if (args.length != 3)
            {
                if (args.length > 3)
                    repError(o, "ERR syntax error");
                else
                    arityErr(o, "xrange");
                break;
            }
            StreamID start, end;
            if (!parseRangeId(args[1].str, true, start) || !parseRangeId(args[2].str, false, end))
            {
                repError(o, "ERR Invalid stream ID specified as stream command argument");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repArrayHeader(o, 0);
                break;
            }
            size_t n = 0;
            obj.stream.walkRange(start, end, limit, (id, pairs) { n++; return 0; });
            repArrayHeader(o, n);
            obj.stream.walkRange(start, end, limit, (id, pairs) {
                repEntry(o, id, pairs);
                return 0;
            });
            break;
        }
    case "XREAD":
        {
            xread(ks, args, o);
            break;
        }
    case "XREVRANGE":
        {
            import dreads.streamops : xrevrange;

            xrevrange(ks, args, o, arena);
            break;
        }
    case "XSETID":
        {
            import dreads.streamops : xsetid;

            xsetid(ks, args, o);
            break;
        }
    case "XINFO":
        {
            import dreads.streamops : xinfo;

            xinfo(ks, args, o);
            break;
        }
    case "XGROUP":
        {
            import dreads.streamops : xgroup;

            xgroup(ks, args, o);
            break;
        }
    case "XREADGROUP":
        {
            import dreads.streamops : xreadgroup;

            xreadgroup(ks, args, o, arena);
            break;
        }
    case "XACK":
        {
            import dreads.streamops : xack;

            xack(ks, args, o);
            break;
        }
    case "XPENDING":
        {
            import dreads.streamops : xpending;

            xpending(ks, args, o);
            break;
        }
    case "XCLAIM":
        {
            import dreads.streamops : xclaim;

            xclaim(ks, args, o);
            break;
        }
    case "XDEL":
        {
            if (args.length < 2)
            {
                arityErr(o, "xdel");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            long n = 0;
            foreach (ref a; args[1 .. $])
            {
                StreamID id;
                if (!parseStreamId(a.str, 0, id))
                {
                    repError(o, "ERR Invalid stream ID specified as stream command argument");
                    return true;
                }
                if (obj !is null && obj.stream.removeId(id))
                    n++;
            }
            repInt(o, n);
            break;
        }
    case "XTRIM":
        {
            // XTRIM key MAXLEN [~|=] n   (the ~ approximation flag is accepted, exact trim applied)
            if (args.length < 3 || args.length > 4 || !eqICKeyword(args[1].str, "MAXLEN"))
            {
                repError(o, "ERR syntax error");
                break;
            }
            auto nArg = args[$ - 1].str;
            if (args.length == 4 && args[2].str != "~" && args[2].str != "=")
            {
                repError(o, "ERR syntax error");
                break;
            }
            long maxlen;
            if (!parseLong(nArg, maxlen) || maxlen < 0)
            {
                repError(o, "ERR value is not an integer or out of range");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.stream, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            repInt(o, obj is null ? 0 : cast(long) obj.stream.trimMaxLen(cast(size_t) maxlen));
            break;
        }

        // --- generic / server batch ---
    case "TOUCH":
        {
            if (args.length == 0)
            {
                arityErr(o, "touch");
                break;
            }
            long n = 0;
            foreach (ref a; args)
                n += ks.exists(a.str) ? 1 : 0;
            repInt(o, n);
            break;
        }
    case "RANDOMKEY":
        {
            // deterministic "random": the first live, unexpired slot
            auto now = nowMs();
            foreach (i; 0 .. ks.d.capacity)
            {
                if (!ks.d.slotLive(i))
                    continue;
                auto obj = ks.d.valAt(i);
                if (obj.expireAtMs != 0 && now >= obj.expireAtMs)
                    continue;
                repBulk(o, ks.d.keyAt(i));
                return true;
            }
            repNullBulk(o);
            break;
        }
    case "COPY":
        {
            if (args.length < 2 || args.length > 3)
            {
                arityErr(o, "copy");
                break;
            }
            bool replace = args.length == 3 && eqICKeyword(args[2].str, "REPLACE");
            if (args.length == 3 && !replace)
            {
                repError(o, "ERR syntax error");
                break;
            }
            auto src = ks.lookup(args[0].str);
            if (src is null || (!replace && ks.exists(args[1].str)))
            {
                repInt(o, 0);
                break;
            }
            auto copy = src.deepDup(); // src pointer dies on the next line's rehash
            ks.d.set(args[1].str, copy);
            repInt(o, 1);
            break;
        }
    case "EXPIRETIME":
    case "PEXPIRETIME":
        {
            if (args.length != 1)
            {
                arityErr(o, name.length == 10 ? "expiretime" : "pexpiretime");
                break;
            }
            auto obj = ks.lookup(args[0].str);
            if (obj is null)
                repInt(o, -2);
            else if (obj.expireAtMs == 0)
                repInt(o, -1);
            else
                repInt(o, name.length == 10 ? cast(long)(obj.expireAtMs / 1000)
                        : cast(long) obj.expireAtMs);
            break;
        }
    case "MOVE":
        {
            if (args.length != 2)
                arityErr(o, "move");
            else if (args[1].str == "0")
                repError(o, "ERR source and destination objects are the same");
            else
                repError(o, "ERR DB index is out of range");
            break;
        }
    case "SWAPDB":
        {
            if (args.length == 2 && args[0].str == "0" && args[1].str == "0")
                repSimple(o, "OK");
            else
                repError(o, "ERR DB index is out of range");
            break;
        }
    case "WAIT":
        {
            repInt(o, 0); // no replicas until Raft lands
            break;
        }
    case "OBJECT":
        {
            objectCmd(ks, args, o);
            break;
        }
    case "LOLWUT":
        {
            repBulk(o, "dreads ⚡ Deadly Fast Redis in DLang\n");
            break;
        }
    case "ROLE":
        {
            repArrayHeader(o, 3);
            repBulk(o, "master");
            repInt(o, 0);
            repArrayHeader(o, 0);
            break;
        }
    case "AUTH":
        {
            repError(o,
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
            break;
        }
    case "SLOWLOG":
        {
            if (args.length >= 1 && eqICKeyword(args[0].str, "RESET"))
                repSimple(o, "OK");
            else if (args.length >= 1 && eqICKeyword(args[0].str, "LEN"))
                repInt(o, 0);
            else
                repArrayHeader(o, 0); // GET / HELP
            break;
        }
    case "LATENCY":
        {
            if (args.length >= 1 && eqICKeyword(args[0].str, "RESET"))
                repInt(o, 0);
            else
                repArrayHeader(o, 0); // LATEST / HISTORY
            break;
        }
    case "MODULE":
        {
            repArrayHeader(o, 0);
            break;
        }
    case "ACL":
        {
            if (args.length >= 1 && eqICKeyword(args[0].str, "WHOAMI"))
                repBulk(o, "default");
            else if (args.length >= 1 && eqICKeyword(args[0].str, "LIST"))
            {
                repArrayHeader(o, 1);
                repBulk(o, "user default on nopass ~* &* +@all");
            }
            else if (args.length >= 1 && eqICKeyword(args[0].str, "CAT"))
                repArrayHeader(o, 0);
            else
                repError(o, "ERR Unknown ACL subcommand");
            break;
        }

        // --- string extras (batch) ---
    case "SUBSTR": // deprecated alias of GETRANGE
        goto case "GETRANGE";
    case "HMSET": // deprecated HSET variant replying +OK
        {
            if (args.length < 3 || (args.length - 1) % 2 != 0)
            {
                arityErr(o, "hmset");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            for (size_t i = 1; i < args.length; i += 2)
                obj.hash.set(args[i].str, StrVal.of(args[i + 1].str));
            repSimple(o, "OK");
            break;
        }
    case "LPUSHX":
    case "RPUSHX":
        {
            if (args.length < 2)
            {
                arityErr(o, nbuf[0] == 'L' ? "lpushx" : "rpushx");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            if (obj is null)
            {
                repInt(o, 0);
                break;
            }
            foreach (ref a; args[1 .. $])
            {
                if (nbuf[0] == 'L')
                    obj.list.pushFront(a.str);
                else
                    obj.list.pushBack(a.str);
            }
            repInt(o, cast(long) obj.list.length);
            break;
        }
    case "HINCRBYFLOAT":
        {
            if (args.length != 3)
            {
                arityErr(o, "hincrbyfloat");
                break;
            }
            double delta;
            if (!parseDouble(args[2].str, delta))
            {
                repError(o, "ERR value is not a valid float");
                break;
            }
            bool wrong;
            auto obj = ks.getOrCreate(args[0].str, ObjType.hash, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            double cur = 0;
            auto f = obj.hash.get(args[1].str);
            if (f !is null && !parseDouble(f.s, cur))
            {
                repError(o, "ERR hash value is not a float");
                break;
            }
            auto nv = cur + delta;
            if (nv != nv || nv == double.infinity || nv == -double.infinity)
            {
                repError(o, "ERR increment would produce NaN or Infinity");
                break;
            }
            char[40] b = void;
            auto res = fmtDouble(b, nv);
            obj.hash.set(args[1].str, StrVal.of(res));
            repBulk(o, res);
            // float math is logged as its result, never re-derived
            propagationOverride.clear();
            repArrayHeader(propagationOverride, 4);
            repBulk(propagationOverride, "HSET");
            repBulk(propagationOverride, args[0].str);
            repBulk(propagationOverride, args[1].str);
            repBulk(propagationOverride, res);
            break;
        }

        // --- misc tail: lists, sort, lcs, hash rand, hll ---
    case "LPOS":
        {
            import dreads.miscops : lpos;

            lpos(ks, args, o, arena);
            break;
        }
    case "LMPOP":
        {
            import dreads.miscops : lmpop;

            lmpop(ks, args, o);
            break;
        }
    case "SORT":
    case "SORT_RO":
        {
            import dreads.miscops : sortCmd;

            sortCmd(ks, args, o, arena, name.length == 7);
            break;
        }
    case "LCS":
        {
            import dreads.miscops : lcs;

            lcs(ks, args, o, arena);
            break;
        }
    case "HRANDFIELD":
        {
            import dreads.miscops : hrandfield;

            hrandfield(ks, args, o);
            break;
        }
    case "PFADD":
        {
            import dreads.hll : pfadd;

            pfadd(ks, args, o);
            break;
        }
    case "PFCOUNT":
        {
            import dreads.hll : pfcount;

            pfcount(ks, args, o);
            break;
        }
    case "PFMERGE":
        {
            import dreads.hll : pfmerge;

            pfmerge(ks, args, o);
            break;
        }

        // --- bitmaps ---
    case "SETBIT":
        {
            import dreads.bitmap : setbit;

            setbit(ks, args, o);
            break;
        }
    case "GETBIT":
        {
            import dreads.bitmap : getbit;

            getbit(ks, args, o);
            break;
        }
    case "BITCOUNT":
        {
            import dreads.bitmap : bitcount;

            bitcount(ks, args, o);
            break;
        }
    case "BITPOS":
        {
            import dreads.bitmap : bitpos;

            bitpos(ks, args, o);
            break;
        }
    case "BITOP":
        {
            import dreads.bitmap : bitop;

            bitop(ks, args, o, arena);
            break;
        }
    case "BITFIELD":
    case "BITFIELD_RO":
        {
            import dreads.bitmap : bitfield;

            bitfield(ks, args, o, arena, name.length == 11);
            break;
        }

        // --- geo ---
    case "GEOADD":
        {
            import dreads.geo : geoadd;

            geoadd(ks, args, o);
            break;
        }
    case "GEOPOS":
        {
            import dreads.geo : geopos;

            geopos(ks, args, o);
            break;
        }
    case "GEODIST":
        {
            import dreads.geo : geodist;

            geodist(ks, args, o);
            break;
        }
    case "GEOHASH":
        {
            import dreads.geo : geohashCmd;

            geohashCmd(ks, args, o);
            break;
        }
    case "GEOSEARCH":
        {
            import dreads.geo : geosearch;

            geosearch(ks, args, o, arena);
            break;
        }
    case "GEOSEARCHSTORE":
        {
            import dreads.geo : geosearchstore;

            geosearchstore(ks, args, o, arena);
            break;
        }
    case "GEORADIUS":
    case "GEORADIUS_RO":
    case "GEORADIUSBYMEMBER":
    case "GEORADIUSBYMEMBER_RO":
        {
            import dreads.geo : georadius;

            bool byMember = name.length == 17 || name.length == 20;
            bool readOnly = name.length == 12 || name.length == 20;
            georadius(ks, args, o, arena, byMember, readOnly);
            break;
        }

        // --- cursor iteration ---
    case "SCAN":
        {
            if (args.length == 0)
            {
                arityErr(o, "scan");
                break;
            }
            long cursor;
            const(char)[] pat;
            long count;
            if (!parseLong(args[0].str, cursor) || cursor < 0
                    || !parseScanOpts(args[1 .. $], pat, count, false))
            {
                repError(o, "ERR syntax error");
                break;
            }
            auto cap = ks.d.capacity;
            auto i = cast(size_t) cursor > cap ? cap : cast(size_t) cursor;
            auto found = arena.allocArray!(const(char)[])(cast(size_t) count);
            size_t got, examined;
            auto now = nowMs();
            while (i < cap && examined < cast(size_t) count)
            {
                if (ks.d.slotLive(i))
                {
                    examined++;
                    auto obj = ks.d.valAt(i);
                    bool dead = obj.expireAtMs != 0 && now >= obj.expireAtMs;
                    if (!dead && (pat.length == 0 || globMatch(pat, ks.d.keyAt(i))))
                        found[got++] = ks.d.keyAt(i);
                }
                i++;
            }
            scanReply(o, i >= cap ? 0 : i, found[0 .. got]);
            break;
        }
    case "HSCAN":
    case "SSCAN":
        {
            bool isHash = nbuf[0] == 'H';
            if (args.length < 2)
            {
                arityErr(o, isHash ? "hscan" : "sscan");
                break;
            }
            long cursor;
            const(char)[] pat;
            long count;
            bool noValues;
            if (!parseLong(args[1].str, cursor) || cursor < 0
                    || !parseScanOpts(args[2 .. $], pat, count, isHash, &noValues))
            {
                repError(o, "ERR syntax error");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, isHash ? ObjType.hash : ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            bool withValues = isHash && !noValues;
            auto found = arena.allocArray!(const(char)[])(cast(size_t) count * (withValues ? 2 : 1));
            size_t got, examined;
            size_t i = cast(size_t) cursor;
            size_t cap = 0;
            if (obj !is null)
            {
                cap = isHash ? obj.hash.capacity : obj.set.capacity;
                if (i > cap)
                    i = cap;
                while (i < cap && examined < cast(size_t) count)
                {
                    bool live = isHash ? obj.hash.slotLive(i) : obj.set.slotLive(i);
                    if (live)
                    {
                        examined++;
                        auto k = isHash ? obj.hash.keyAt(i) : obj.set.keyAt(i);
                        if (pat.length == 0 || globMatch(pat, k))
                        {
                            found[got++] = k;
                            if (withValues)
                                found[got++] = obj.hash.valAt(i).s;
                        }
                    }
                    i++;
                }
            }
            scanReply(o, i >= cap ? 0 : i, found[0 .. got]);
            break;
        }
    case "ZSCAN":
        {
            // rank-based cursor: ordered, no slot-layout dependence
            if (args.length < 2)
            {
                arityErr(o, "zscan");
                break;
            }
            long cursor;
            const(char)[] pat;
            long count;
            if (!parseLong(args[1].str, cursor) || cursor < 0
                    || !parseScanOpts(args[2 .. $], pat, count, false))
            {
                repError(o, "ERR syntax error");
                break;
            }
            bool wrong;
            auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
            if (wrong)
            {
                repWrongType(o);
                break;
            }
            auto len = obj is null ? 0 : obj.zset.length;
            auto start = cast(size_t) cursor > len ? len : cast(size_t) cursor;
            auto n = cast(size_t) count < len - start ? cast(size_t) count : len - start;
            size_t matched = 0;
            if (n)
                obj.zset.walkRange(start, n, false, (m, s) {
                    if (pat.length == 0 || globMatch(pat, m))
                        matched++;
                    return 0;
                });
            char[24] cb = void;
            auto next = start + n >= len ? 0 : start + n;
            auto cn = snprintf(cb.ptr, cb.length, "%llu", cast(ulong) next);
            repArrayHeader(o, 2);
            repBulk(o, cb[0 .. cn]);
            repArrayHeader(o, matched * 2);
            if (n)
                obj.zset.walkRange(start, n, false, (m, s) {
                    if (pat.length == 0 || globMatch(pat, m))
                    {
                        repBulk(o, m);
                        repDouble(o, s);
                    }
                    return 0;
                });
            break;
        }

    default:
        unknownCmd(o, name);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Command helpers
// ---------------------------------------------------------------------------

private void incrDecr(ref Keyspace ks, scope const(char)[] key, long delta, ref ByteBuffer o) @nogc nothrow
{
    bool wrong;
    auto obj = ks.lookupTyped(key, ObjType.str, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    long cur = 0;
    if (obj !is null && !parseLong(obj.str.s, cur))
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    bool ovf;
    auto nv = adds(cur, delta, ovf);
    if (ovf)
    {
        repError(o, "ERR increment or decrement would overflow");
        return;
    }
    char[24] buf = void;
    auto n = snprintf(buf.ptr, buf.length, "%lld", nv);
    ks.setStr(key, buf[0 .. n]);
    repInt(o, nv);
}

private void listPop(ref Keyspace ks, const(RVal)[] args, bool fromHead, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1 || args.length > 2)
    {
        arityErr(o, fromHead ? "lpop" : "rpop");
        return;
    }
    long howMany = 1;
    bool withCount = args.length == 2;
    if (withCount && (!parseLong(args[1].str, howMany) || howMany < 0))
    {
        repError(o, "ERR value is out of range, must be positive");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.list, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    if (obj is null)
    {
        if (withCount)
            o.append("*-1\r\n");
        else
            repNullBulk(o);
        return;
    }
    auto n = cast(size_t)(howMany < cast(long) obj.list.length ? howMany : obj.list.length);
    if (withCount)
        repArrayHeader(o, n);
    foreach (_; 0 .. n)
    {
        repBulk(o, fromHead ? obj.list.front : obj.list.back);
        if (fromHead)
            obj.list.popFront();
        else
            obj.list.popBack();
    }
    ks.delIfEmpty(args[0].str, obj);
}

private bool parseSide(scope const(char)[] s, out bool left) @nogc nothrow
{
    if (eqICKeyword(s, "LEFT"))
    {
        left = true;
        return true;
    }
    return eqICKeyword(s, "RIGHT");
}

/// LMOVE / RPOPLPUSH. Careful with pointer invalidation: getOrCreate may
/// rehash the keyspace, so the source pointer is not used after it.
private void lmove(ref Keyspace ks, scope const(char)[] src, scope const(char)[] dst,
        bool fromLeft, bool toLeft, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    bool w1, w2;
    auto s = ks.lookupTyped(src, ObjType.list, w1);
    ks.lookupTyped(dst, ObjType.list, w2);
    if (w1 || w2)
    {
        repWrongType(o);
        return;
    }
    if (s is null)
    {
        repNullBulk(o);
        return;
    }
    auto v = arena.dupString(fromLeft ? s.list.front : s.list.back);
    if (fromLeft)
        s.list.popFront();
    else
        s.list.popBack();
    bool w3;
    auto d = ks.getOrCreate(dst, ObjType.list, w3); // s is stale from here on
    if (toLeft)
        d.list.pushFront(v);
    else
        d.list.pushBack(v);
    auto s2 = ks.lookup(src);
    if (s2 !is null)
        ks.delIfEmpty(src, s2);
    repBulk(o, v);
}

/// SPOP: deterministic pop of the first live slots, but always propagated as
/// SREM of the removed members so the log never depends on table layout.
private void spop(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 1 || args.length > 2)
    {
        arityErr(o, "spop");
        return;
    }
    long howMany = 1;
    bool withCount = args.length == 2;
    if (withCount && (!parseLong(args[1].str, howMany) || howMany < 0))
    {
        repError(o, "ERR value is out of range, must be positive");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    if (obj is null)
    {
        if (withCount)
            repArrayHeader(o, 0);
        else
            repNullBulk(o);
        return;
    }
    auto n = cast(size_t)(howMany < cast(long) obj.set.length ? howMany : obj.set.length);
    auto members = arena.allocArray!(const(char)[])(n);
    size_t got = 0;
    foreach (i; 0 .. obj.set.capacity)
    {
        if (got == n)
            break;
        if (obj.set.slotLive(i))
            members[got++] = arena.dupString(obj.set.keyAt(i));
    }
    if (withCount)
        repArrayHeader(o, n);
    foreach (m; members[0 .. n])
        repBulk(o, m);
    propagationOverride.clear();
    repArrayHeader(propagationOverride, n + 2);
    repBulk(propagationOverride, "SREM");
    repBulk(propagationOverride, args[0].str);
    foreach (m; members[0 .. n])
    {
        repBulk(propagationOverride, m);
        obj.set.del(m);
    }
    ks.delIfEmpty(args[0].str, obj);
}

private void srandmember(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1 || args.length > 2)
    {
        arityErr(o, "srandmember");
        return;
    }
    long howMany = 1;
    bool withCount = args.length == 2;
    if (withCount && !parseLong(args[1].str, howMany))
    {
        repError(o, "ERR value is not an integer or out of range");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.set, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    if (obj is null || obj.set.length == 0)
    {
        if (withCount)
            repArrayHeader(o, 0);
        else
            repNullBulk(o);
        return;
    }
    if (!withCount)
    {
        foreach (i; 0 .. obj.set.capacity)
        {
            if (obj.set.slotLive(i))
            {
                repBulk(o, obj.set.keyAt(i));
                break;
            }
        }
        return;
    }
    // positive count: up to card distinct members; negative: cycle with repeats
    bool repeat = howMany < 0;
    auto want = cast(size_t)(repeat ? -howMany : howMany);
    auto n = repeat ? want : (want < obj.set.length ? want : obj.set.length);
    repArrayHeader(o, n);
    size_t emitted = 0;
    while (emitted < n)
    {
        foreach (i; 0 .. obj.set.capacity)
        {
            if (emitted == n)
                break;
            if (obj.set.slotLive(i))
            {
                repBulk(o, obj.set.keyAt(i));
                emitted++;
            }
        }
    }
}

private void sintercard(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    long numkeys;
    if (args.length < 2 || !parseLong(args[0].str, numkeys) || numkeys < 1
            || args.length < 1 + cast(size_t) numkeys)
    {
        repError(o, "ERR numkeys should be greater than 0");
        return;
    }
    long limit = 0;
    auto rest = args[1 + cast(size_t) numkeys .. $];
    if (rest.length == 2 && eqICKeyword(rest[0].str, "LIMIT"))
    {
        if (!parseLong(rest[1].str, limit) || limit < 0)
        {
            repError(o, "ERR LIMIT can't be negative");
            return;
        }
    }
    else if (rest.length != 0)
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto keys = args[1 .. 1 + cast(size_t) numkeys];
    auto sets = arena.allocArray!(const(Dict!Unit)*)(keys.length);
    foreach (i, ref a; keys)
    {
        bool wrong;
        auto obj = ks.lookupTyped(a.str, ObjType.set, wrong);
        if (wrong)
        {
            repWrongType(o);
            return;
        }
        if (obj is null)
        {
            repInt(o, 0);
            return;
        }
        sets[i] = &obj.set;
    }
    long n = 0;
    outer: foreach (i; 0 .. sets[0].capacity)
    {
        if (!sets[0].slotLive(i))
            continue;
        auto m = sets[0].keyAt(i);
        foreach (s; sets[1 .. $])
        {
            if (!s.exists(m))
                continue outer;
        }
        n++;
        if (limit && n == limit)
            break;
    }
    repInt(o, n);
}

/// SINTERSTORE / SUNIONSTORE / SDIFFSTORE (op: 'I', 'U', 'D').
private void setStore(ref Keyspace ks, const(RVal)[] args, char op,
        ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 2)
    {
        arityErr(o, op == 'I' ? "sinterstore" : (op == 'U' ? "sunionstore" : "sdiffstore"));
        return;
    }
    auto srcs = args[1 .. $];
    Dict!Unit tmp;
    if (op == 'U')
    {
        foreach (ref a; srcs)
        {
            bool wrong;
            auto obj = ks.lookupTyped(a.str, ObjType.set, wrong);
            if (wrong)
            {
                tmp.free();
                repWrongType(o);
                return;
            }
            if (obj is null)
                continue;
            foreach (m, ref u; obj.set)
                tmp.set(m, Unit());
        }
    }
    else
    {
        auto others = arena.allocArray!(const(Dict!Unit)*)(srcs.length - 1);
        const(Dict!Unit)* base;
        foreach (i, ref a; srcs)
        {
            bool wrong;
            auto obj = ks.lookupTyped(a.str, ObjType.set, wrong);
            if (wrong)
            {
                repWrongType(o);
                return;
            }
            auto setp = obj is null ? null : &obj.set;
            if (i == 0)
                base = setp;
            else
                others[i - 1] = setp;
        }
        if (base !is null)
        {
            bool inter = op == 'I';
            outer2: foreach (i; 0 .. base.capacity)
            {
                if (!base.slotLive(i))
                    continue;
                auto m = base.keyAt(i);
                foreach (s; others)
                {
                    bool inOther = s !is null && s.exists(m);
                    if (inter != inOther)
                        continue outer2;
                }
                tmp.set(m, Unit());
            }
        }
    }
    auto card = tmp.length;
    if (card == 0)
    {
        tmp.free();
        ks.del(args[0].str);
    }
    else
    {
        RObj obj;
        obj.type = ObjType.set;
        obj.set = tmp; // ownership moves into the keyspace
        ks.d.set(args[0].str, obj);
    }
    repInt(o, cast(long) card);
}

/// ZPOPMIN / ZPOPMAX with optional count.
private void zpop(ref Keyspace ks, const(RVal)[] args, bool popMax,
        ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 1 || args.length > 2)
    {
        arityErr(o, popMax ? "zpopmax" : "zpopmin");
        return;
    }
    long howMany = 1;
    if (args.length == 2 && (!parseLong(args[1].str, howMany) || howMany < 0))
    {
        repError(o, "ERR value is out of range, must be positive");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    auto n = obj is null ? 0
        : cast(size_t)(howMany < cast(long) obj.zset.length ? howMany : obj.zset.length);
    repArrayHeader(o, n * 2);
    foreach (_; 0 .. n)
    {
        const(char)[] victim;
        obj.zset.walkRange(0, 1, popMax, (m, s) {
            repBulk(o, m);
            repDouble(o, s);
            victim = arena.dupString(m);
            return 0;
        });
        obj.zset.remove(victim);
    }
    if (obj !is null)
        ks.delIfEmpty(args[0].str, obj);
}

/// ZREMRANGEBYRANK (byRank=true) / ZREMRANGEBYSCORE.
private void zremrange(ref Keyspace ks, const(RVal)[] args, bool byRank,
        ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length != 3)
    {
        arityErr(o, byRank ? "zremrangebyrank" : "zremrangebyscore");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongType(o);
        return;
    }
    if (obj is null)
    {
        repInt(o, 0);
        return;
    }
    auto victims = arena.allocArray!(const(char)[])(obj.zset.length);
    size_t n = 0;
    if (byRank)
    {
        long start, stop;
        if (!parseLong(args[1].str, start) || !parseLong(args[2].str, stop))
        {
            repError(o, "ERR value is not an integer or out of range");
            return;
        }
        auto len = cast(long) obj.zset.length;
        normalizeRange(start, stop, len);
        if (start <= stop)
            obj.zset.walkRange(cast(size_t) start, cast(size_t)(stop - start + 1), false, (m, s) {
                victims[n++] = arena.dupString(m);
                return 0;
            });
    }
    else
    {
        double min, max;
        bool minExcl, maxExcl;
        if (!parseScoreBound(args[1].str, min, minExcl) || !parseScoreBound(args[2].str,
                max, maxExcl))
        {
            repError(o, "ERR min or max is not a float");
            return;
        }
        obj.zset.walkScoreRange(min, minExcl, max, maxExcl, (m, s) {
            victims[n++] = arena.dupString(m);
            return 0;
        });
    }
    foreach (m; victims[0 .. n])
        obj.zset.remove(m);
    ks.delIfEmpty(args[0].str, obj);
    repInt(o, cast(long) n);
}

/// SINTER (inter=true) / SDIFF (inter=false): first set filtered by the rest.
private void setCombine(ref Keyspace ks, const(RVal)[] args, bool inter,
        ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length == 0)
    {
        arityErr(o, inter ? "sinter" : "sdiff");
        return;
    }
    auto others = arena.allocArray!(const(Dict!Unit)*)(args.length - 1);
    const(Dict!Unit)* base;
    foreach (i, ref a; args)
    {
        bool wrong;
        auto obj = ks.lookupTyped(a.str, ObjType.set, wrong);
        if (wrong)
        {
            repWrongType(o);
            return;
        }
        auto setp = obj is null ? null : &obj.set;
        if (i == 0)
            base = setp;
        else
            others[i - 1] = setp;
    }
    if (base is null)
    {
        repArrayHeader(o, 0);
        return;
    }
    bool keepMember(const(char)[] m) @nogc nothrow
    {
        foreach (s; others)
        {
            bool inOther = s !is null && s.exists(m);
            if (inter != inOther)
                return false;
        }
        return true;
    }

    size_t n = 0;
    foreach (i; 0 .. base.capacity)
    {
        if (base.slotLive(i) && keepMember(base.keyAt(i)))
            n++;
    }
    repArrayHeader(o, n);
    foreach (i; 0 .. base.capacity)
    {
        if (base.slotLive(i) && keepMember(base.keyAt(i)))
            repBulk(o, base.keyAt(i));
    }
}

private void setUnion(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length == 0)
    {
        arityErr(o, "sunion");
        return;
    }
    Dict!Unit acc;
    scope (exit)
        acc.free();
    foreach (ref a; args)
    {
        bool wrong;
        auto obj = ks.lookupTyped(a.str, ObjType.set, wrong);
        if (wrong)
        {
            repWrongType(o);
            return;
        }
        if (obj is null)
            continue;
        foreach (m, ref u; obj.set)
            acc.set(m, Unit());
    }
    repArrayHeader(o, acc.length);
    foreach (m, ref u; acc)
        repBulk(o, m);
}

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

/// "ms" or "ms-seq"; a bare "ms" gets seqDefault.
private bool parseStreamId(scope const(char)[] s, ulong seqDefault, out StreamID id) @nogc nothrow
{
    size_t dash = size_t.max;
    foreach (i, c; s)
    {
        if (c == '-' && i > 0)
        {
            dash = i;
            break;
        }
    }
    long ms, seq;
    if (dash == size_t.max)
    {
        if (!parseLong(s, ms) || ms < 0)
            return false;
        id = StreamID(cast(ulong) ms, seqDefault);
        return true;
    }
    if (!parseLong(s[0 .. dash], ms) || ms < 0)
        return false;
    if (!parseLong(s[dash + 1 .. $], seq) || seq < 0)
        return false;
    id = StreamID(cast(ulong) ms, cast(ulong) seq);
    return true;
}

/// XRANGE bounds: "-" and "+" specials; bare "ms" is inclusive on both ends.
private bool parseRangeId(scope const(char)[] s, bool isStart, out StreamID id) @nogc nothrow
{
    if (s == "-")
    {
        id = StreamID.minId;
        return true;
    }
    if (s == "+")
    {
        id = StreamID.maxId;
        return true;
    }
    return parseStreamId(s, isStart ? 0 : ulong.max, id);
}

private void repStreamId(ref ByteBuffer o, StreamID id) @nogc nothrow
{
    char[48] buf = void;
    auto n = snprintf(buf.ptr, buf.length, "%llu-%llu", id.ms, id.seq);
    repBulk(o, buf[0 .. n]);
}

/// *2 [id][*2k f1 v1 ...]
private void repEntry(ref ByteBuffer o, StreamID id, const(FieldPair)[] pairs) @nogc nothrow
{
    repArrayHeader(o, 2);
    repStreamId(o, id);
    repArrayHeader(o, pairs.length * 2);
    foreach (ref p; pairs)
    {
        repBulk(o, p.field);
        repBulk(o, p.value);
    }
}

/// [MATCH pat] [COUNT n] [NOVALUES] tail options of the SCAN family.
private bool parseScanOpts(const(RVal)[] opts, out const(char)[] pat, out long count,
        bool allowNoValues, bool* noValues = null) @nogc nothrow
{
    count = 10;
    size_t i = 0;
    while (i < opts.length)
    {
        if (eqICKeyword(opts[i].str, "MATCH") && i + 1 < opts.length)
        {
            pat = opts[i + 1].str;
            i += 2;
        }
        else if (eqICKeyword(opts[i].str, "COUNT") && i + 1 < opts.length)
        {
            if (!parseLong(opts[i + 1].str, count) || count < 1)
                return false;
            i += 2;
        }
        else if (allowNoValues && noValues !is null && eqICKeyword(opts[i].str, "NOVALUES"))
        {
            *noValues = true;
            i++;
        }
        else
            return false;
    }
    return true;
}

/// *2 [next-cursor][items...]
private void scanReply(ref ByteBuffer o, size_t nextCursor, const(const(char)[])[] items) @nogc nothrow
{
    char[24] b = void;
    auto n = snprintf(b.ptr, b.length, "%llu", cast(ulong) nextCursor);
    repArrayHeader(o, 2);
    repBulk(o, b[0 .. n]);
    repArrayHeader(o, items.length);
    foreach (it; items)
        repBulk(o, it);
}

public bool eqICKeyword(scope const(char)[] s, scope const(char)[] upper) @nogc nothrow
{
    if (s.length != upper.length)
        return false;
    foreach (i, c; s)
    {
        auto u = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
        if (u != upper[i])
            return false;
    }
    return true;
}

/// XREAD [COUNT n] STREAMS key [key ...] id [id ...] — non-blocking only.
/// Entries strictly greater than the given id; "$" means the stream's lastId.
private void xread(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    size_t i = 0;
    size_t limit = 0;
    if (args.length >= 2 && eqICKeyword(args[0].str, "COUNT"))
    {
        long n;
        if (!parseLong(args[1].str, n) || n < 0)
        {
            repError(o, "ERR value is not an integer or out of range");
            return;
        }
        limit = cast(size_t) n;
        i = 2;
    }
    if (i >= args.length || !eqICKeyword(args[i].str, "STREAMS"))
    {
        repError(o, "ERR syntax error");
        return;
    }
    auto rest = args[i + 1 .. $];
    if (rest.length == 0 || rest.length % 2 != 0)
    {
        repError(o,
                "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.");
        return;
    }
    auto half = rest.length / 2;

    // resolve the exclusive start id per stream and count streams with data
    bool startFor(size_t k, out StreamID start, out const(RObj)* obj) @nogc nothrow
    {
        bool wrong;
        obj = ks.lookupTyped(rest[k].str, ObjType.stream, wrong);
        if (wrong || obj is null)
            return false; // wrong type surfaces below; missing stream has no data
        StreamID after;
        auto spec = rest[half + k].str;
        if (spec == "$")
            after = obj.stream.lastId;
        else if (!parseStreamId(spec, 0, after))
            return false;
        // strictly greater: bump to the next possible id
        start = after.seq == ulong.max ? StreamID(after.ms + 1, 0) : StreamID(after.ms,
                after.seq + 1);
        return true;
    }

    // validate every id and stream type upfront (errors beat partial replies)
    foreach (k; 0 .. half)
    {
        auto spec = rest[half + k].str;
        StreamID ignored;
        if (spec != "$" && !parseStreamId(spec, 0, ignored))
        {
            repError(o, "ERR Invalid stream ID specified as stream command argument");
            return;
        }
        bool wrong;
        ks.lookupTyped(rest[k].str, ObjType.stream, wrong);
        if (wrong)
        {
            repWrongType(o);
            return;
        }
    }

    size_t withData = 0;
    foreach (k; 0 .. half)
    {
        StreamID start;
        const(RObj)* obj;
        if (!startFor(k, start, obj))
            continue;
        size_t n = 0;
        obj.stream.walkRange(start, StreamID.maxId, 1, (id, pairs) { n++; return 0; });
        if (n > 0)
            withData++;
    }
    if (withData == 0)
    {
        o.append("*-1\r\n");
        return;
    }
    repArrayHeader(o, withData);
    foreach (k; 0 .. half)
    {
        StreamID start;
        const(RObj)* obj;
        if (!startFor(k, start, obj))
            continue;
        size_t n = 0;
        obj.stream.walkRange(start, StreamID.maxId, limit, (id, pairs) { n++; return 0; });
        if (n == 0)
            continue;
        repArrayHeader(o, 2);
        repBulk(o, rest[k].str);
        repArrayHeader(o, n);
        obj.stream.walkRange(start, StreamID.maxId, limit, (id, pairs) {
            repEntry(o, id, pairs);
            return 0;
        });
    }
}

/// OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ (introspection; reports OUR encodings).
private void objectCmd(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length != 2)
    {
        repError(o, "ERR wrong number of arguments for 'object' command");
        return;
    }
    auto obj = ks.lookup(args[1].str);
    if (obj is null)
    {
        repError(o, "ERR no such key");
        return;
    }
    auto sub = args[0].str;
    if (eqICKeyword(sub, "ENCODING"))
    {
        final switch (obj.type)
        {
        case ObjType.str:
            long v;
            repBulk(o, parseLong(obj.str.s, v) ? "int" : "raw");
            break;
        case ObjType.list:
            repBulk(o, "linkedlist");
            break;
        case ObjType.hash:
            repBulk(o, "hashtable");
            break;
        case ObjType.set:
            repBulk(o, "hashtable");
            break;
        case ObjType.zset:
            repBulk(o, "skiplist");
            break;
        case ObjType.stream:
            repBulk(o, "stream");
            break;
        }
    }
    else if (eqICKeyword(sub, "REFCOUNT"))
        repInt(o, 1);
    else if (eqICKeyword(sub, "IDLETIME"))
        repInt(o, 0);
    else if (eqICKeyword(sub, "FREQ"))
        repError(o, "ERR An LFU maxmemory policy is not selected, access frequency not tracked");
    else
        repError(o, "ERR Unknown OBJECT subcommand");
}

// ---------------------------------------------------------------------------
// Expiry helpers
// ---------------------------------------------------------------------------

/// Resolves an expire argument to absolute epoch ms, checking for overflow.
private bool resolveExpireMs(long v, bool isSec, bool isRel, ref long absMs) @nogc nothrow
{
    import core.checkedint : adds, muls;

    bool ovf;
    long ms = isSec ? muls(v, 1000, ovf) : v;
    if (isRel)
        ms = adds(ms, cast(long) nowMs(), ovf);
    if (ovf)
        return false;
    absMs = ms;
    return true;
}

private void propagatePexpireat(scope const(char)[] key, ulong absMs) @nogc nothrow
{
    propagationOverride.clear();
    repArrayHeader(propagationOverride, 3);
    repBulk(propagationOverride, "PEXPIREAT");
    repBulk(propagationOverride, key);
    char[24] b = void;
    auto n = snprintf(b.ptr, b.length, "%llu", absMs);
    repBulk(propagationOverride, b[0 .. n]);
}

private void propagatePersist(scope const(char)[] key) @nogc nothrow
{
    propagationOverride.clear();
    repArrayHeader(propagationOverride, 2);
    repBulk(propagationOverride, "PERSIST");
    repBulk(propagationOverride, key);
}

/// SET with any option is logged canonically: SET key val [PXAT absMs].
private void propagateSet(scope const(char)[] key, scope const(char)[] val, ulong absMs) @nogc nothrow
{
    propagationOverride.clear();
    repArrayHeader(propagationOverride, absMs != 0 ? 5 : 3);
    repBulk(propagationOverride, "SET");
    repBulk(propagationOverride, key);
    repBulk(propagationOverride, val);
    if (absMs != 0)
    {
        repBulk(propagationOverride, "PXAT");
        char[24] b = void;
        auto n = snprintf(b.ptr, b.length, "%llu", absMs);
        repBulk(propagationOverride, b[0 .. n]);
    }
}

// ---------------------------------------------------------------------------
// Small parsing / reply utilities
// ---------------------------------------------------------------------------

/// LRANGE/ZRANGE index normalization: negatives count from the end, then clamp.
public void normalizeRange(ref long start, ref long stop, long len) @nogc nothrow
{
    if (start < 0)
        start += len;
    if (stop < 0)
        stop += len;
    if (start < 0)
        start = 0;
    if (stop >= len)
        stop = len - 1;
}

public bool parseLong(scope const(char)[] s, out long v) @nogc nothrow
{
    if (s.length == 0)
        return false;
    size_t i = 0;
    bool neg = false;
    if (s[0] == '-' || s[0] == '+')
    {
        neg = s[0] == '-';
        i = 1;
        if (s.length == 1)
            return false;
    }
    ulong acc = 0;
    for (; i < s.length; i++)
    {
        auto c = s[i];
        if (c < '0' || c > '9')
            return false;
        auto digit = cast(ulong)(c - '0');
        if (acc > (ulong.max - digit) / 10)
            return false;
        acc = acc * 10 + digit;
    }
    if (neg)
    {
        if (acc > cast(ulong) long.max + 1)
            return false;
        v = acc == cast(ulong) long.max + 1 ? long.min : -cast(long) acc;
    }
    else
    {
        if (acc > cast(ulong) long.max)
            return false;
        v = cast(long) acc;
    }
    return true;
}

public bool parseDouble(scope const(char)[] s, out double v) @nogc nothrow
{
    char[64] tmp = void;
    if (s.length == 0 || s.length >= tmp.length)
        return false;
    memcpy(tmp.ptr, s.ptr, s.length);
    tmp[s.length] = 0;
    char* endp;
    v = strtod(tmp.ptr, &endp);
    if (endp !is tmp.ptr + s.length)
        return false;
    return v == v; // reject NaN like Redis
}

public bool parseScoreBound(scope const(char)[] s, out double v, out bool excl) @nogc nothrow
{
    if (s.length > 0 && s[0] == '(')
    {
        excl = true;
        s = s[1 .. $];
    }
    return parseDouble(s, v);
}

private bool eqICWithScores(scope const(char)[] s) @nogc nothrow
{
    if (s.length != 10)
        return false;
    static immutable up = "WITHSCORES";
    foreach (i, c; s)
    {
        auto u = c >= 'a' && c <= 'z' ? cast(char)(c - 32) : c;
        if (u != up[i])
            return false;
    }
    return true;
}

/// Redis's double formatting: integers print without decimals.
private const(char)[] fmtDouble(return ref char[40] buf, double d) @nogc nothrow
{
    int n;
    if (d == cast(long) d && d > -1e17 && d < 1e17)
        n = snprintf(buf.ptr, buf.length, "%lld", cast(long) d);
    else
        n = snprintf(buf.ptr, buf.length, "%.17g", d);
    return buf[0 .. n];
}

public void repDouble(ref ByteBuffer o, double d) @nogc nothrow
{
    char[40] buf = void;
    repBulk(o, fmtDouble(buf, d));
}

private void repWrongType(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}

private void arityErr(ref ByteBuffer o, scope const(char)[] cmdLower) @nogc nothrow
{
    o.append("-ERR wrong number of arguments for '");
    o.append(cmdLower);
    o.append("' command\r\n");
}

private void unknownCmd(ref ByteBuffer o, scope const(char)[] name) @nogc nothrow
{
    o.append("-ERR unknown command '");
    appendSanitized(o, name);
    o.append("'\r\n");
}

/// Client-provided text going into an error reply must not inject CRLF.
private void appendSanitized(ref ByteBuffer o, scope const(char)[] s) @nogc nothrow
{
    if (s.length > 128)
        s = s[0 .. 128];
    foreach (c; s)
        o.appendByte(c == '\r' || c == '\n' ? ' ' : c);
}

/// True when a command mutates the keyspace (drives AOF logging).
/// Takes the already-uppercased name.
public bool isWriteCommand(scope const(char)[] uname) @nogc nothrow
{
    switch (uname)
    {
    case "SET", "SETNX", "GETSET", "APPEND", "INCR", "DECR", "INCRBY", "DECRBY", "MSET":
    case "SETEX", "PSETEX", "GETDEL", "SETRANGE", "INCRBYFLOAT", "MSETNX":
    case "DEL", "UNLINK", "FLUSHALL", "FLUSHDB", "RENAME", "RENAMENX", "COPY":
    case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST":
    case "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LREM", "LPUSHX", "RPUSHX":
    case "LTRIM", "LINSERT", "LMOVE", "RPOPLPUSH":
    case "HSET", "HMSET", "HDEL", "HINCRBY", "HSETNX", "HINCRBYFLOAT":
    case "SADD", "SREM", "SPOP", "SMOVE", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE":
    case "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZMPOP":
    case "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "ZRANGESTORE":
    case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
    case "XADD", "XDEL", "XTRIM", "XSETID", "XGROUP", "XREADGROUP", "XACK", "XCLAIM":
    case "GEOADD", "GEOSEARCHSTORE", "GEORADIUS", "GEORADIUSBYMEMBER":
    case "SETBIT", "BITOP", "BITFIELD":
    case "LMPOP", "SORT", "PFADD", "PFMERGE":
        return true;
    default:
        return false;
    }
}

/// Redis-style glob: * matches any run, ? matches one char.
public bool globMatch(scope const(char)[] pat, scope const(char)[] s) @nogc nothrow
{
    size_t p, i;
    size_t star = size_t.max, mark;
    while (i < s.length)
    {
        if (p < pat.length && (pat[p] == '?' || pat[p] == s[i]))
        {
            p++;
            i++;
        }
        else if (p < pat.length && pat[p] == '*')
        {
            star = p++;
            mark = i;
        }
        else if (star != size_t.max)
        {
            p = star + 1;
            i = ++mark;
        }
        else
            return false;
    }
    while (p < pat.length && pat[p] == '*')
        p++;
    return p == pat.length;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

version (unittest)
{
    private string respCmd(string[] args...)
    {
        import std.conv : to;

        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
        return r;
    }

    private string runCmd(ref Keyspace ks, string encoded, bool* keep = null)
    {
        Arena a;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear(); // never inherit a previous test's override
        assert(parseValue(cast(const(ubyte)[]) encoded, pos, a, v) == ParseStatus.ok);
        auto k = dispatch(v, ks, o, a);
        if (keep !is null)
            *keep = k;
        return (cast(string) o.data).idup;
    }

    private string run(ref Keyspace ks, string[] args...)
    {
        return runCmd(ks, respCmd(args));
    }
}

unittest // connection basics, unknown, QUIT
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "PING") == "+PONG\r\n");
    assert(run(ks, "ping", "oi") == "$2\r\noi\r\n");
    assert(run(ks, "ECHO", "hello") == "$5\r\nhello\r\n");
    assert(run(ks, "COMMAND", "DOCS") == "*0\r\n");
    assert(run(ks, "NOSUCH") == "-ERR unknown command 'NOSUCH'\r\n");
    bool keep;
    runCmd(ks, respCmd("QUIT"), &keep);
    assert(!keep);
}

unittest // strings
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "SET", "k", "v") == "+OK\r\n");
    assert(run(ks, "GET", "k") == "$1\r\nv\r\n");
    assert(run(ks, "GET", "nope") == "$-1\r\n");
    assert(run(ks, "SETNX", "k", "other") == ":0\r\n");
    assert(run(ks, "SETNX", "k2", "x") == ":1\r\n");
    assert(run(ks, "GETSET", "k", "new") == "$1\r\nv\r\n");
    assert(run(ks, "GET", "k") == "$3\r\nnew\r\n");
    assert(run(ks, "APPEND", "k", "!") == ":4\r\n");
    assert(run(ks, "GET", "k") == "$4\r\nnew!\r\n");
    assert(run(ks, "APPEND", "fresh", "hi") == ":2\r\n");
    assert(run(ks, "STRLEN", "k") == ":4\r\n");
    assert(run(ks, "STRLEN", "nope") == ":0\r\n");
    assert(run(ks, "INCR", "n") == ":1\r\n");
    assert(run(ks, "INCRBY", "n", "41") == ":42\r\n");
    assert(run(ks, "DECR", "n") == ":41\r\n");
    assert(run(ks, "DECRBY", "n", "40") == ":1\r\n");
    assert(run(ks, "INCR", "k") == "-ERR value is not an integer or out of range\r\n");
    assert(run(ks, "SET", "big", "9223372036854775807") == "+OK\r\n");
    assert(run(ks, "INCR", "big") == "-ERR increment or decrement would overflow\r\n");
    assert(run(ks, "MSET", "a", "1", "b", "2") == "+OK\r\n");
    assert(run(ks, "MGET", "a", "b", "ghost") == "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n");
    assert(run(ks, "MSET", "a") == "-ERR wrong number of arguments for 'mset' command\r\n");
}

unittest // lists
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "RPUSH", "l", "b", "c") == ":2\r\n");
    assert(run(ks, "LPUSH", "l", "a") == ":3\r\n");
    assert(run(ks, "LLEN", "l") == ":3\r\n");
    assert(run(ks, "LRANGE", "l", "0", "-1") == "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    assert(run(ks, "LRANGE", "l", "-2", "-1") == "*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    assert(run(ks, "LRANGE", "l", "5", "10") == "*0\r\n");
    assert(run(ks, "LINDEX", "l", "1") == "$1\r\nb\r\n");
    assert(run(ks, "LINDEX", "l", "-1") == "$1\r\nc\r\n");
    assert(run(ks, "LINDEX", "l", "9") == "$-1\r\n");
    assert(run(ks, "LSET", "l", "1", "B") == "+OK\r\n");
    assert(run(ks, "LINDEX", "l", "1") == "$1\r\nB\r\n");
    assert(run(ks, "LSET", "l", "9", "x") == "-ERR index out of range\r\n");
    assert(run(ks, "LSET", "ghost", "0", "x") == "-ERR no such key\r\n");
    assert(run(ks, "LPOP", "l") == "$1\r\na\r\n");
    assert(run(ks, "RPOP", "l") == "$1\r\nc\r\n");
    assert(run(ks, "LPOP", "l", "5") == "*1\r\n$1\r\nB\r\n");
    assert(run(ks, "EXISTS", "l") == ":0\r\n"); // emptied lists vanish
    assert(run(ks, "LPOP", "ghost") == "$-1\r\n");
    assert(run(ks, "LPOP", "ghost", "2") == "*-1\r\n");
    run(ks, "RPUSH", "r", "x", "y", "x", "x");
    assert(run(ks, "LREM", "r", "2", "x") == ":2\r\n");
    assert(run(ks, "LRANGE", "r", "0", "-1") == "*2\r\n$1\r\ny\r\n$1\r\nx\r\n");
}

unittest // hashes
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "HSET", "h", "f1", "v1", "f2", "v2") == ":2\r\n");
    assert(run(ks, "HSET", "h", "f1", "changed") == ":0\r\n");
    assert(run(ks, "HGET", "h", "f1") == "$7\r\nchanged\r\n");
    assert(run(ks, "HGET", "h", "nope") == "$-1\r\n");
    assert(run(ks, "HMGET", "h", "f2", "nope") == "*2\r\n$2\r\nv2\r\n$-1\r\n");
    assert(run(ks, "HLEN", "h") == ":2\r\n");
    assert(run(ks, "HEXISTS", "h", "f1") == ":1\r\n");
    assert(run(ks, "HEXISTS", "h", "nah") == ":0\r\n");
    auto all = run(ks, "HGETALL", "h");
    assert(all[0 .. 4] == "*4\r\n");
    import std.algorithm : canFind;

    assert(all.canFind("$2\r\nf1\r\n$7\r\nchanged\r\n"));
    assert(all.canFind("$2\r\nf2\r\n$2\r\nv2\r\n"));
    assert(run(ks, "HKEYS", "h")[0 .. 4] == "*2\r\n");
    assert(run(ks, "HVALS", "h")[0 .. 4] == "*2\r\n");
    assert(run(ks, "HINCRBY", "h", "count", "5") == ":5\r\n");
    assert(run(ks, "HINCRBY", "h", "count", "-2") == ":3\r\n");
    assert(run(ks, "HINCRBY", "h", "f1", "1") == "-ERR hash value is not an integer\r\n");
    assert(run(ks, "HDEL", "h", "f1", "f2", "count") == ":3\r\n");
    assert(run(ks, "EXISTS", "h") == ":0\r\n"); // emptied hashes vanish
}

unittest // sets
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "SADD", "s1", "a", "b", "c", "a") == ":3\r\n");
    assert(run(ks, "SADD", "s2", "b", "c", "d") == ":3\r\n");
    assert(run(ks, "SCARD", "s1") == ":3\r\n");
    assert(run(ks, "SISMEMBER", "s1", "a") == ":1\r\n");
    assert(run(ks, "SISMEMBER", "s1", "z") == ":0\r\n");
    assert(run(ks, "SMEMBERS", "ghost") == "*0\r\n");
    auto inter = run(ks, "SINTER", "s1", "s2");
    assert(inter[0 .. 4] == "*2\r\n"); // b, c
    auto diff = run(ks, "SDIFF", "s1", "s2");
    assert(diff == "*1\r\n$1\r\na\r\n");
    auto uni = run(ks, "SUNION", "s1", "s2");
    assert(uni[0 .. 4] == "*4\r\n"); // a b c d
    assert(run(ks, "SINTER", "s1", "ghost") == "*0\r\n");
    assert(run(ks, "SREM", "s1", "a", "z") == ":1\r\n");
    assert(run(ks, "SREM", "s1", "b", "c") == ":2\r\n");
    assert(run(ks, "EXISTS", "s1") == ":0\r\n"); // emptied sets vanish
}

unittest // sorted sets
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "ZADD", "z", "1", "a", "2", "b", "3", "c") == ":3\r\n");
    assert(run(ks, "ZADD", "z", "10", "b") == ":0\r\n"); // update
    assert(run(ks, "ZCARD", "z") == ":3\r\n");
    assert(run(ks, "ZSCORE", "z", "b") == "$2\r\n10\r\n");
    assert(run(ks, "ZSCORE", "z", "half") == "$-1\r\n");
    run(ks, "ZADD", "z", "0.5", "half");
    assert(run(ks, "ZSCORE", "z", "half") == "$3\r\n0.5\r\n");
    assert(run(ks, "ZRANK", "z", "half") == ":0\r\n");
    assert(run(ks, "ZRANK", "z", "b") == ":3\r\n");
    assert(run(ks, "ZREVRANK", "z", "b") == ":0\r\n");
    assert(run(ks, "ZRANK", "z", "ghost") == "$-1\r\n");
    assert(run(ks, "ZRANGE", "z", "0", "-1") == "*4\r\n$4\r\nhalf\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\nb\r\n");
    assert(run(ks, "ZREVRANGE", "z", "0", "1") == "*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    assert(run(ks, "ZRANGE", "z", "0", "0", "WITHSCORES") == "*2\r\n$4\r\nhalf\r\n$3\r\n0.5\r\n");
    assert(run(ks, "ZRANGEBYSCORE", "z", "1", "3") == "*2\r\n$1\r\na\r\n$1\r\nc\r\n");
    assert(run(ks, "ZRANGEBYSCORE", "z", "(1", "+inf") == "*2\r\n$1\r\nc\r\n$1\r\nb\r\n");
    assert(run(ks, "ZRANGEBYSCORE", "z", "-inf", "+inf")[0 .. 4] == "*4\r\n");
    assert(run(ks, "ZINCRBY", "z", "2.5", "a") == "$3\r\n3.5\r\n");
    assert(run(ks, "ZADD", "z", "notanumber", "x") == "-ERR value is not a valid float\r\n");
    assert(run(ks, "ZREM", "z", "a", "ghost") == ":1\r\n");
    run(ks, "ZREM", "z", "b", "c", "half");
    assert(run(ks, "EXISTS", "z") == ":0\r\n"); // emptied zsets vanish
}

unittest // TYPE and WRONGTYPE
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    run(ks, "SET", "s", "v");
    run(ks, "RPUSH", "l", "v");
    run(ks, "HSET", "h", "f", "v");
    run(ks, "SADD", "st", "v");
    run(ks, "ZADD", "z", "1", "v");
    assert(run(ks, "TYPE", "s") == "+string\r\n");
    assert(run(ks, "TYPE", "l") == "+list\r\n");
    assert(run(ks, "TYPE", "h") == "+hash\r\n");
    assert(run(ks, "TYPE", "st") == "+set\r\n");
    assert(run(ks, "TYPE", "z") == "+zset\r\n");
    assert(run(ks, "TYPE", "ghost") == "+none\r\n");

    enum wt = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    assert(run(ks, "GET", "l") == wt);
    assert(run(ks, "LPUSH", "s", "x") == wt);
    assert(run(ks, "HGET", "l", "f") == wt);
    assert(run(ks, "SADD", "z", "m") == wt);
    assert(run(ks, "ZADD", "st", "1", "m") == wt);
    assert(run(ks, "INCR", "l") == wt);
    // SET overwrites regardless of type, like Redis
    assert(run(ks, "SET", "l", "now-string") == "+OK\r\n");
    assert(run(ks, "TYPE", "l") == "+string\r\n");
}

unittest // KEYS with glob patterns over the typed keyspace
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    run(ks, "SET", "user:1", "a");
    run(ks, "RPUSH", "user:2", "b");
    run(ks, "SET", "other", "c");
    auto reply = run(ks, "KEYS", "user:*");
    assert(reply[0 .. 4] == "*2\r\n");
    assert(run(ks, "KEYS", "nada*") == "*0\r\n");
    assert(run(ks, "KEYS", "u?er:1")[0 .. 4] == "*1\r\n");
    assert(run(ks, "DBSIZE") == ":3\r\n");
    assert(run(ks, "FLUSHALL") == "+OK\r\n");
    assert(run(ks, "DBSIZE") == ":0\r\n");
}

unittest // TTL / expiry
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    run(ks, "SET", "k", "v");
    assert(run(ks, "TTL", "k") == ":-1\r\n");
    assert(run(ks, "TTL", "ghost") == ":-2\r\n");
    assert(run(ks, "EXPIRE", "k", "100") == ":1\r\n");
    // EXPIRE must be propagated as absolute PEXPIREAT (read before the next
    // command clears the override)
    auto ov = (cast(string) propagationOverride.data).idup;
    assert(ov[0 .. 18] == "*3\r\n$9\r\nPEXPIREAT\r");
    assert(run(ks, "TTL", "k") == ":100\r\n");
    auto pttl = run(ks, "PTTL", "k"); // ~100000ms remaining
    assert(pttl[0 .. 3] == ":99" || pttl[0 .. 4] == ":100");
    assert(run(ks, "PERSIST", "k") == ":1\r\n");
    assert(run(ks, "PERSIST", "k") == ":0\r\n");
    assert(run(ks, "TTL", "k") == ":-1\r\n");
    // expiry in the past: key reads as gone everywhere
    assert(run(ks, "PEXPIREAT", "k", "1") == ":1\r\n");
    assert(run(ks, "GET", "k") == "$-1\r\n");
    assert(run(ks, "EXISTS", "k") == ":0\r\n");
    assert(run(ks, "TTL", "k") == ":-2\r\n");
    assert(run(ks, "DEL", "k") == ":0\r\n"); // already gone
    assert(run(ks, "EXPIRE", "nokey", "10") == ":0\r\n");
}

unittest // SET options, SETEX, GETEX, GETDEL
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "SET", "k", "v1", "NX") == "+OK\r\n");
    assert(run(ks, "SET", "k", "v2", "NX") == "$-1\r\n");
    assert(run(ks, "GET", "k") == "$2\r\nv1\r\n");
    assert(run(ks, "SET", "k", "v3", "XX") == "+OK\r\n");
    assert(run(ks, "SET", "novo", "x", "XX") == "$-1\r\n");
    assert(run(ks, "SET", "k", "v4", "GET") == "$2\r\nv3\r\n");
    assert(run(ks, "SET", "k", "v5", "EX", "100") == "+OK\r\n");
    // SET with options is propagated canonically with PXAT (read before the
    // next command clears the override)
    auto ov = (cast(string) propagationOverride.data).idup;
    import std.algorithm : canFind;

    assert(ov[0 .. 4] == "*5\r\n" && ov.canFind("PXAT"));
    assert(run(ks, "TTL", "k") == ":100\r\n");
    assert(run(ks, "SET", "k", "v6", "KEEPTTL") == "+OK\r\n");
    assert(run(ks, "TTL", "k") == ":100\r\n"); // preserved
    assert(run(ks, "SET", "k", "v7") == "+OK\r\n");
    assert(run(ks, "TTL", "k") == ":-1\r\n"); // plain SET clears TTL
    assert(run(ks, "SET", "k", "v", "EX", "0") == "-ERR invalid expire time in 'set' command\r\n");
    assert(run(ks, "SET", "k", "v", "NX", "XX") == "-ERR syntax error\r\n");
    assert(run(ks, "SET", "k", "v", "BOGUS") == "-ERR syntax error\r\n");

    assert(run(ks, "SETEX", "s", "50", "val") == "+OK\r\n");
    assert(run(ks, "TTL", "s") == ":50\r\n");
    assert(run(ks, "SETEX", "s", "0", "val") == "-ERR invalid expire time in 'setex' command\r\n");

    assert(run(ks, "GETEX", "s") == "$3\r\nval\r\n"); // plain read
    assert(run(ks, "GETEX", "s", "PERSIST") == "$3\r\nval\r\n");
    assert(run(ks, "TTL", "s") == ":-1\r\n");
    assert(run(ks, "GETEX", "s", "EX", "70") == "$3\r\nval\r\n");
    assert(run(ks, "TTL", "s") == ":70\r\n");
    assert(run(ks, "GETEX", "ghost") == "$-1\r\n");

    assert(run(ks, "GETDEL", "s") == "$3\r\nval\r\n");
    assert(run(ks, "EXISTS", "s") == ":0\r\n");
    assert(run(ks, "GETDEL", "s") == "$-1\r\n");
}

unittest // XADD * fills propagationOverride with the resolved ID
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    auto reply = run(ks, "XADD", "st", "*", "f", "v");
    assert(reply[0] == '$');
    auto ov = (cast(string) propagationOverride.data).idup;
    import std.algorithm : canFind;

    assert(ov[0 .. 4] == "*5\r\n");
    assert(ov.canFind("$4\r\nXADD\r\n"));
    assert(!ov.canFind("$1\r\n*\r\n")); // the literal "*" never reaches the log
}

unittest // streams: XADD/XLEN/XRANGE
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    assert(run(ks, "XADD", "s", "1-1", "a", "1") == "$3\r\n1-1\r\n");
    assert(run(ks, "XADD", "s", "1-2", "b", "2") == "$3\r\n1-2\r\n");
    assert(run(ks, "XADD", "s", "5", "c", "3") == "$3\r\n5-0\r\n"); // bare ms
    assert(run(ks, "XADD", "s", "1-1", "d", "4")
            == "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
    assert(run(ks, "XADD", "s", "0-0", "d", "4")
            == "-ERR The ID specified in XADD must be greater than 0-0\r\n");
    assert(run(ks, "XADD", "s", "abc", "d", "4")
            == "-ERR Invalid stream ID specified as stream command argument\r\n");
    assert(run(ks, "XLEN", "s") == ":3\r\n");
    assert(run(ks, "XLEN", "ghost") == ":0\r\n");

    // auto id must be strictly increasing and parseable
    auto autoId = run(ks, "XADD", "s", "*", "e", "5");
    assert(autoId[0] == '$');

    assert(run(ks, "XRANGE", "s", "1", "1")
            == "*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\n1-2\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n");
    assert(run(ks, "XRANGE", "s", "-", "+")[0 .. 4] == "*4\r\n");
    assert(run(ks, "XRANGE", "s", "-", "+", "COUNT", "2")[0 .. 4] == "*2\r\n");
    assert(run(ks, "XRANGE", "ghost", "-", "+") == "*0\r\n");
    assert(run(ks, "TYPE", "s") == "+stream\r\n");

    enum wt = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    run(ks, "SET", "str", "x");
    assert(run(ks, "XADD", "str", "1-1", "a", "b") == wt);
    assert(run(ks, "XRANGE", "str", "-", "+") == wt);
}

unittest // streams: XREAD
{
    Keyspace ks;
    scope (exit)
        ks.d.free();
    run(ks, "XADD", "s1", "1-1", "a", "1");
    run(ks, "XADD", "s1", "2-1", "b", "2");
    run(ks, "XADD", "s2", "9-0", "z", "9");

    // entries strictly after 1-1
    assert(run(ks, "XREAD", "STREAMS", "s1", "1-1")
            == "*1\r\n*2\r\n$2\r\ns1\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n");
    // nothing after the tip -> nil
    assert(run(ks, "XREAD", "STREAMS", "s1", "2-1") == "*-1\r\n");
    assert(run(ks, "XREAD", "STREAMS", "s1", "$") == "*-1\r\n");
    // two streams, one with data
    auto two = run(ks, "XREAD", "STREAMS", "s1", "s2", "2-1", "0");
    assert(two[0 .. 4] == "*1\r\n");
    import std.algorithm : canFind;

    assert(two.canFind("s2") && two.canFind("9-0"));
    // COUNT limits per stream
    assert(run(ks, "XREAD", "COUNT", "1", "STREAMS", "s1", "0")[0 .. 4] == "*1\r\n");
    assert(run(ks, "XREAD", "STREAMS", "s1") ==
            "-ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.\r\n");
    assert(run(ks, "XREAD", "s1", "0") == "-ERR syntax error\r\n");
}

unittest // globMatch corners
{
    assert(globMatch("*", ""));
    assert(globMatch("*", "anything"));
    assert(globMatch("a*c", "abc"));
    assert(globMatch("a*c", "ac"));
    assert(!globMatch("a*c", "ab"));
    assert(globMatch("a?c", "abc"));
    assert(!globMatch("a?c", "ac"));
    assert(globMatch("*fim", "no fim"));
    assert(!globMatch("", "x"));
    assert(globMatch("", ""));
}
