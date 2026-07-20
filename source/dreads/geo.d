module dreads.geo;

// GEO commands. As in Redis, a geo key IS a sorted set whose score is the
// 52-bit interleaved geohash of (longitude, latitude) — every zset command
// works on geo keys. Searches scan the set and filter with haversine
// distances: O(n) but exact; cell pruning is a later optimization.

import core.stdc.math : asin, cos, sin, sqrt;
import core.stdc.stdio : snprintf;
import core.stdc.stdlib : qsort;

import dreads.commands : eqICKeyword, parseDouble, parseLong;
import dreads.mem : Arena, ByteBuffer;
import dreads.obj : Keyspace, ObjType, RObj;
import dreads.resp;

// Redis's exact constants (geohash.c / geohash_helper.c)
private enum GEO_LAT_MIN = -85.05112878;
private enum GEO_LAT_MAX = 85.05112878;
private enum GEO_LON_MIN = -180.0;
private enum GEO_LON_MAX = 180.0;
private enum GEO_STEP = 26;
private enum EARTH_RADIUS_M = 6_372_797.560856;
private enum PI = 3.141592653589793;

// ---------------------------------------------------------------------------
// 52-bit geohash
// ---------------------------------------------------------------------------

private ulong spreadBits(ulong x) @nogc nothrow
{
    x &= 0xFFFF_FFFFUL;
    x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFFUL;
    x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FFUL;
    x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0FUL;
    x = (x | (x << 2)) & 0x3333_3333_3333_3333UL;
    x = (x | (x << 1)) & 0x5555_5555_5555_5555UL;
    return x;
}

private ulong squashBits(ulong x) @nogc nothrow
{
    x &= 0x5555_5555_5555_5555UL;
    x = (x | (x >> 1)) & 0x3333_3333_3333_3333UL;
    x = (x | (x >> 2)) & 0x0F0F_0F0F_0F0F_0F0FUL;
    x = (x | (x >> 4)) & 0x00FF_00FF_00FF_00FFUL;
    x = (x | (x >> 8)) & 0x0000_FFFF_0000_FFFFUL;
    x = (x | (x >> 16)) & 0x0000_0000_FFFF_FFFFUL;
    return x;
}

/// Redis layout: latitude in the even bit positions, longitude in the odd.
public ulong geohashEncode(double lon, double lat) @nogc nothrow
{
    auto latOff = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    auto lonOff = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
    auto ilat = cast(ulong)(latOff * (1UL << GEO_STEP));
    auto ilon = cast(ulong)(lonOff * (1UL << GEO_STEP));
    return spreadBits(ilat) | (spreadBits(ilon) << 1);
}

/// Cell-center decode.
public void geohashDecode(ulong bits, out double lon, out double lat) @nogc nothrow
{
    auto ilat = squashBits(bits);
    auto ilon = squashBits(bits >> 1);
    enum scale = 1.0 / (1UL << GEO_STEP);
    auto latMin = GEO_LAT_MIN + ilat * scale * (GEO_LAT_MAX - GEO_LAT_MIN);
    auto latMax = GEO_LAT_MIN + (ilat + 1) * scale * (GEO_LAT_MAX - GEO_LAT_MIN);
    auto lonMin = GEO_LON_MIN + ilon * scale * (GEO_LON_MAX - GEO_LON_MIN);
    auto lonMax = GEO_LON_MIN + (ilon + 1) * scale * (GEO_LON_MAX - GEO_LON_MIN);
    lat = (latMin + latMax) / 2;
    lon = (lonMin + lonMax) / 2;
}

public double haversine(double lon1, double lat1, double lon2, double lat2) @nogc nothrow
{
    auto lat1r = lat1 * PI / 180.0;
    auto lat2r = lat2 * PI / 180.0;
    auto u = sin((lat2r - lat1r) / 2);
    auto v = sin((lon2 - lon1) * PI / 180.0 / 2);
    return 2.0 * EARTH_RADIUS_M * asin(sqrt(u * u + cos(lat1r) * cos(lat2r) * v * v));
}

private bool unitFactor(scope const(char)[] u, out double toMeters) @nogc nothrow
{
    if (eqICKeyword(u, "M"))
        toMeters = 1;
    else if (eqICKeyword(u, "KM"))
        toMeters = 1000;
    else if (eqICKeyword(u, "MI"))
        toMeters = 1609.34;
    else if (eqICKeyword(u, "FT"))
        toMeters = 0.3048;
    else
        return false;
    return true;
}

private bool validCoords(double lon, double lat) @nogc nothrow
{
    return lon >= GEO_LON_MIN && lon <= GEO_LON_MAX && lat >= GEO_LAT_MIN && lat <= GEO_LAT_MAX;
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]
public void geoadd(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 4)
    {
        repError(o, "ERR wrong number of arguments for 'geoadd' command");
        return;
    }
    bool nx, xx, ch;
    size_t i = 1;
    while (i < args.length)
    {
        if (eqICKeyword(args[i].str, "NX"))
            nx = true;
        else if (eqICKeyword(args[i].str, "XX"))
            xx = true;
        else if (eqICKeyword(args[i].str, "CH"))
            ch = true;
        else
            break;
        i++;
    }
    auto rest = args[i .. $];
    if ((nx && xx) || rest.length == 0 || rest.length % 3 != 0)
    {
        repError(o, "ERR syntax error");
        return;
    }
    // validate every triple before touching the keyspace
    foreach (t; 0 .. rest.length / 3)
    {
        double lon, lat;
        if (!parseDouble(rest[t * 3].str, lon) || !parseDouble(rest[t * 3 + 1].str, lat))
        {
            repError(o, "ERR value is not a valid float");
            return;
        }
        if (!validCoords(lon, lat))
        {
            repError(o, "ERR invalid longitude,latitude pair");
            return;
        }
    }
    bool wrong;
    auto obj = ks.getOrCreate(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    long added, changed;
    foreach (t; 0 .. rest.length / 3)
    {
        double lon, lat;
        parseDouble(rest[t * 3].str, lon);
        parseDouble(rest[t * 3 + 1].str, lat);
        auto member = rest[t * 3 + 2].str;
        auto score = cast(double) geohashEncode(lon, lat);
        double cur;
        bool exists = obj.zset.score(member, cur);
        if ((nx && exists) || (xx && !exists))
            continue;
        if (obj.zset.add(score, member))
            added++;
        if (!exists || cur != score)
            changed++;
    }
    repInt(o, ch ? changed : added);
}

/// GEOPOS key member [member ...]
public void geopos(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'geopos' command");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    repArrayHeader(o, args.length - 1);
    foreach (ref a; args[1 .. $])
    {
        double s;
        if (obj is null || !obj.zset.score(a.str, s))
        {
            repNullArray(o);
            continue;
        }
        double lon, lat;
        geohashDecode(cast(ulong) s, lon, lat);
        repArrayHeader(o, 2);
        repCoord(o, lon);
        repCoord(o, lat);
    }
}

/// GEODIST key m1 m2 [unit]
public void geodist(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 3 || args.length > 4)
    {
        repError(o, "ERR wrong number of arguments for 'geodist' command");
        return;
    }
    double factor = 1;
    if (args.length == 4 && !unitFactor(args[3].str, factor))
    {
        repError(o, "ERR unsupported unit provided. please use M, KM, FT, MI");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    double s1, s2;
    if (obj is null || !obj.zset.score(args[1].str, s1) || !obj.zset.score(args[2].str, s2))
    {
        repNullBulk(o);
        return;
    }
    double lon1, lat1, lon2, lat2;
    geohashDecode(cast(ulong) s1, lon1, lat1);
    geohashDecode(cast(ulong) s2, lon2, lat2);
    char[40] b = void;
    auto n = snprintf(b.ptr, b.length, "%.4f", haversine(lon1, lat1, lon2, lat2) / factor);
    repBulk(o, b[0 .. n]);
}

/// GEOHASH key member [member ...] — standard 11-char geohash strings.
public void geohashCmd(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o) @nogc nothrow
{
    if (args.length < 1)
    {
        repError(o, "ERR wrong number of arguments for 'geohash' command");
        return;
    }
    bool wrong;
    auto obj = ks.lookupTyped(args[0].str, ObjType.zset, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    static immutable alphabet = "0123456789bcdefghjkmnpqrstuvwxyz";
    repArrayHeader(o, args.length - 1);
    foreach (ref a; args[1 .. $])
    {
        double s;
        if (obj is null || !obj.zset.score(a.str, s))
        {
            repNullBulk(o);
            continue;
        }
        double lon, lat;
        geohashDecode(cast(ulong) s, lon, lat);
        // like Redis: 52 bits of bisection over WGS84 ranges (longitude
        // first), then the remaining 3 bits of the 11-char string are zeros
        double lonMin = -180, lonMax = 180, latMin = -90, latMax = 90;
        ulong bits = 0;
        bool even = true;
        foreach (_; 0 .. 52)
        {
            bits <<= 1;
            if (even)
            {
                auto mid = (lonMin + lonMax) / 2;
                if (lon > mid)
                {
                    bits |= 1;
                    lonMin = mid;
                }
                else
                    lonMax = mid;
            }
            else
            {
                auto mid = (latMin + latMax) / 2;
                if (lat > mid)
                {
                    bits |= 1;
                    latMin = mid;
                }
                else
                    latMax = mid;
            }
            even = !even;
        }
        // exactly like Redis: chars 0..9 take bits 51..2 of the 52-bit hash
        // (the last 2 bits are dropped) and the 11th char is always '0'
        char[11] buf;
        foreach (ci; 0 .. 10)
            buf[ci] = alphabet[(bits >> (52 - 5 * (ci + 1))) & 0x1F];
        buf[10] = '0';
        repBulk(o, buf[]);
    }
}

// ---------------------------------------------------------------------------
// Search core (GEOSEARCH / GEOSEARCHSTORE / GEORADIUS*)
// ---------------------------------------------------------------------------

private struct GeoHit
{
    const(char)[] member; // arena copy
    double dist; // meters
    double lon, lat;
    ulong bits;
}

extern (C) private int hitCmpAsc(scope const void* a, scope const void* b) nothrow @nogc
{
    auto x = cast(const(GeoHit)*) a;
    auto y = cast(const(GeoHit)*) b;
    return x.dist < y.dist ? -1 : (x.dist > y.dist ? 1 : 0);
}

extern (C) private int hitCmpDesc(scope const void* l, scope const void* r) nothrow @nogc
{
    return hitCmpAsc(r, l); // descending = ascending with the operands reversed
}

private struct SearchSpec
{
    const(char)[] key;
    bool fromMember;
    const(char)[] member;
    double lon = 0, lat = 0;
    bool byBox;
    double radiusM = -1; // BYRADIUS
    double widthM = -1, heightM = -1; // BYBOX
    double unit = 1; // meters per input unit
    bool asc, desc;
    long count = 0;
    bool countAny;
    bool hasFromLonLat, hasByRadius, hasAny; // for GEOSEARCH origin/shape validation
    bool withCoord, withDist, withHash;
    const(char)[] storeKey; // GEORADIUS STORE
    const(char)[] storeDistKey; // GEORADIUS STOREDIST k / GEOSEARCHSTORE STOREDIST
}

/// Options shared by GEOSEARCH-style commands, starting after the key(s).
private bool parseSearchOpts(const(RVal)[] opts, ref SearchSpec sp, bool allowStore) @nogc nothrow
{
    size_t i = 0;
    while (i < opts.length)
    {
        auto w = opts[i].str;
        if (eqICKeyword(w, "FROMMEMBER") && i + 1 < opts.length)
        {
            sp.fromMember = true;
            sp.member = opts[i + 1].str;
            i += 2;
        }
        else if (eqICKeyword(w, "FROMLONLAT") && i + 2 < opts.length)
        {
            sp.hasFromLonLat = true;
            if (!parseDouble(opts[i + 1].str, sp.lon) || !parseDouble(opts[i + 2].str, sp.lat))
                return false;
            i += 3;
        }
        else if (eqICKeyword(w, "BYRADIUS") && i + 2 < opts.length)
        {
            sp.hasByRadius = true;
            if (!parseDouble(opts[i + 1].str, sp.radiusM) || !unitFactor(opts[i + 2].str, sp.unit))
                return false;
            sp.radiusM *= sp.unit;
            i += 3;
        }
        else if (eqICKeyword(w, "BYBOX") && i + 3 < opts.length)
        {
            sp.byBox = true;
            if (!parseDouble(opts[i + 1].str, sp.widthM)
                    || !parseDouble(opts[i + 2].str, sp.heightM)
                    || !unitFactor(opts[i + 3].str, sp.unit))
                return false;
            sp.widthM *= sp.unit;
            sp.heightM *= sp.unit;
            i += 4;
        }
        else if (eqICKeyword(w, "ASC"))
        {
            sp.asc = true;
            i++;
        }
        else if (eqICKeyword(w, "DESC"))
        {
            sp.desc = true;
            i++;
        }
        else if (eqICKeyword(w, "COUNT") && i + 1 < opts.length)
        {
            if (!parseLong(opts[i + 1].str, sp.count) || sp.count <= 0)
                return false;
            i += 2;
            if (i < opts.length && eqICKeyword(opts[i].str, "ANY"))
            {
                sp.countAny = true;
                sp.hasAny = true;
                i++;
            }
        }
        else if (eqICKeyword(w, "ANY")) // bare ANY (no preceding COUNT) — flagged,
        {
            sp.hasAny = true; // validated to a specific "ANY requires COUNT" error
            i++;
        }
        else if (eqICKeyword(w, "WITHCOORD"))
        {
            sp.withCoord = true;
            i++;
        }
        else if (eqICKeyword(w, "WITHDIST"))
        {
            sp.withDist = true;
            i++;
        }
        else if (eqICKeyword(w, "WITHHASH"))
        {
            sp.withHash = true;
            i++;
        }
        else if (allowStore && eqICKeyword(w, "STORE") && i + 1 < opts.length)
        {
            sp.storeKey = opts[i + 1].str;
            i += 2;
        }
        else if (allowStore && eqICKeyword(w, "STOREDIST") && i + 1 < opts.length)
        {
            sp.storeDistKey = opts[i + 1].str;
            i += 2;
        }
        else
            return false;
    }
    return true;
}

/// Runs the scan+filter; hits are arena-owned. Returns -1 on missing member.
private long runSearch(ref Keyspace ks, ref SearchSpec sp, ref Arena arena,
        out GeoHit[] outHits, out bool wrongType) @nogc nothrow
{
    bool wrong;
    auto obj = ks.lookupTyped(sp.key, ObjType.zset, wrong);
    if (wrong)
    {
        wrongType = true;
        return 0;
    }
    if (obj is null)
    {
        outHits = null;
        return 0;
    }
    if (sp.fromMember)
    {
        double s;
        if (!obj.zset.score(sp.member, s))
            return -1;
        geohashDecode(cast(ulong) s, sp.lon, sp.lat);
    }
    auto hits = arena.allocArray!GeoHit(obj.zset.length);
    size_t n = 0;
    obj.zset.walkRange(0, obj.zset.length, false, (m, s) {
        double lon, lat;
        geohashDecode(cast(ulong) s, lon, lat);
        double dist;
        if (sp.byBox)
        {
            auto lonDist = haversine(lon, lat, sp.lon, lat);
            auto latDist = haversine(lon, lat, lon, sp.lat);
            if (lonDist > sp.widthM / 2 || latDist > sp.heightM / 2)
                return 0;
            dist = haversine(lon, lat, sp.lon, sp.lat);
        }
        else
        {
            dist = haversine(lon, lat, sp.lon, sp.lat);
            if (dist > sp.radiusM)
                return 0;
        }
        hits[n].member = arena.dupString(m);
        hits[n].dist = dist;
        hits[n].lon = lon;
        hits[n].lat = lat;
        hits[n].bits = cast(ulong) s;
        n++;
        // COUNT ... ANY: stop as soon as enough matches exist
        if (sp.countAny && sp.count && n == cast(size_t) sp.count)
            return 1;
        return 0;
    });
    // Sort by distance ONLY when the client asked for order: explicit ASC/DESC,
    // or a plain COUNT (which returns the N CLOSEST → implies a sort). No COUNT
    // and no ASC/DESC → Redis returns the matches UNSORTED (zset/geohash order,
    // which is exactly walkRange's order). COUNT ... ANY is unsorted too, unless
    // ASC/DESC is also present (then the ANY-selected subset is sorted).
    if (sp.asc || sp.desc || (sp.count > 0 && !sp.countAny))
        qsort(hits.ptr, n, GeoHit.sizeof, sp.desc ? &hitCmpDesc : &hitCmpAsc);
    if (sp.count && n > cast(size_t) sp.count)
        n = cast(size_t) sp.count;
    outHits = hits[0 .. n];
    return cast(long) n;
}

private void emitHits(ref ByteBuffer o, const(GeoHit)[] hits, const ref SearchSpec sp) @nogc nothrow
{
    repArrayHeader(o, hits.length);
    bool plain = !sp.withCoord && !sp.withDist && !sp.withHash;
    foreach (ref h; hits)
    {
        if (plain)
        {
            repBulk(o, h.member);
            continue;
        }
        size_t parts = 1 + (sp.withDist ? 1 : 0) + (sp.withHash ? 1 : 0) + (sp.withCoord ? 1 : 0);
        repArrayHeader(o, parts);
        repBulk(o, h.member);
        if (sp.withDist)
        {
            char[40] b = void;
            auto n = snprintf(b.ptr, b.length, "%.4f", h.dist / sp.unit);
            repBulk(o, b[0 .. n]);
        }
        if (sp.withHash)
            repInt(o, cast(long) h.bits);
        if (sp.withCoord)
        {
            repArrayHeader(o, 2);
            repCoord(o, h.lon);
            repCoord(o, h.lat);
        }
    }
}

private void storeHits(ref Keyspace ks, scope const(char)[] dest,
        const(GeoHit)[] hits, bool storeDist, double unit, ref ByteBuffer o) @nogc nothrow
{
    import dreads.zset : ZSet;

    if (hits.length == 0)
    {
        ks.del(dest);
        repInt(o, 0);
        return;
    }
    RObj obj;
    obj.type = ObjType.zset;
    foreach (ref h; hits)
        obj.zset.add(storeDist ? h.dist / unit : cast(double) h.bits, h.member);
    ks.d.set(dest, obj);
    repInt(o, cast(long) hits.length);
}

// "ERR member <m> does not exist" (GEO*BYMEMBER / GEOSEARCH FROMMEMBER miss).
private void repNoMember(ref ByteBuffer o, scope const(char)[] m) @nogc nothrow
{
    o.appendByte('-');
    o.append("ERR member ");
    foreach (ch; m)
        o.appendByte(ch == '\r' || ch == '\n' ? ' ' : ch);
    o.append(" does not exist\r\n");
}

// ANY without COUNT is invalid for every search form. Returns the error text, or
// null. (The pattern `*ANY*requires*COUNT*` is what the suite matches.)
private string anyNeedsCount(ref const SearchSpec sp) @nogc nothrow
{
    return (sp.hasAny && sp.count == 0)
        ? "ERR the ANY argument requires COUNT argument" : null;
}

// GEOSEARCH must have exactly one origin (FROMMEMBER|FROMLONLAT) and one shape
// (BYRADIUS|BYBOX). Returns the error text to emit, or null when valid.
private string geoSearchValidate(ref const SearchSpec sp) @nogc nothrow
{
    if (sp.fromMember && sp.hasFromLonLat)
        return "ERR syntax error";
    if (!sp.fromMember && !sp.hasFromLonLat)
        return "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH";
    if (sp.hasByRadius && sp.byBox)
        return "ERR syntax error";
    if (!sp.hasByRadius && !sp.byBox)
        return "ERR exactly one of BYRADIUS, BYBOX and BYPOLYGON can be specified for GEOSEARCH";
    return null;
}

/// GEOSEARCH key <FROMMEMBER m | FROMLONLAT lon lat> <BYRADIUS r u | BYBOX w h u> [opts]
public void geosearch(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 4)
    {
        repError(o, "ERR wrong number of arguments for 'geosearch' command");
        return;
    }
    SearchSpec sp;
    sp.key = args[0].str;
    if (!parseSearchOpts(args[1 .. $], sp, false))
    {
        repError(o, "ERR syntax error");
        return;
    }
    if (auto e = geoSearchValidate(sp))
    {
        repError(o, e);
        return;
    }
    if (auto e = anyNeedsCount(sp))
    {
        repError(o, e);
        return;
    }
    GeoHit[] hits;
    bool wrong;
    auto r = runSearch(ks, sp, arena, hits, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    if (r < 0)
    {
        repNoMember(o, sp.member);
        return;
    }
    emitHits(o, hits, sp);
}

/// GEOSEARCHSTORE dest src <search options> [STOREDIST]
public void geosearchstore(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o, ref Arena arena) @nogc nothrow
{
    if (args.length < 5)
    {
        repError(o, "ERR wrong number of arguments for 'geosearchstore' command");
        return;
    }
    bool storeDist;
    auto opts = args[2 .. $];
    if (opts.length > 0 && eqICKeyword(opts[$ - 1].str, "STOREDIST"))
    {
        storeDist = true;
        opts = opts[0 .. $ - 1];
    }
    SearchSpec sp;
    sp.key = args[1].str;
    if (!parseSearchOpts(opts, sp, false))
    {
        repError(o, "ERR syntax error");
        return;
    }
    if (auto e = geoSearchValidate(sp))
    {
        repError(o, e);
        return;
    }
    if (auto e = anyNeedsCount(sp))
    {
        repError(o, e);
        return;
    }
    GeoHit[] hits;
    bool wrong;
    auto r = runSearch(ks, sp, arena, hits, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    if (r < 0)
    {
        repNoMember(o, sp.member);
        return;
    }
    storeHits(ks, args[0].str, hits, storeDist, sp.unit, o);
}

/// GEORADIUS[_RO] key lon lat radius unit [opts] and
/// GEORADIUSBYMEMBER[_RO] key member radius unit [opts] — legacy forms.
public void georadius(ref Keyspace ks, const(RVal)[] args, ref ByteBuffer o,
        ref Arena arena, bool byMember, bool readOnly) @nogc nothrow
{
    auto fixed = byMember ? 4 : 5;
    if (args.length < fixed)
    {
        repError(o, "ERR wrong number of arguments");
        return;
    }
    SearchSpec sp;
    sp.key = args[0].str;
    size_t at = 1;
    if (byMember)
    {
        sp.fromMember = true;
        sp.member = args[at++].str;
    }
    else
    {
        if (!parseDouble(args[at].str, sp.lon) || !parseDouble(args[at + 1].str, sp.lat))
        {
            repError(o, "ERR value is not a valid float");
            return;
        }
        at += 2;
    }
    if (!parseDouble(args[at].str, sp.radiusM) || !unitFactor(args[at + 1].str, sp.unit))
    {
        repError(o, "ERR value is not a valid float");
        return;
    }
    sp.radiusM *= sp.unit;
    at += 2;
    if (!parseSearchOpts(args[at .. $], sp, !readOnly))
    {
        repError(o, "ERR syntax error");
        return;
    }
    if (auto e = anyNeedsCount(sp))
    {
        repError(o, e);
        return;
    }
    // STORE/STOREDIST write a plain zset — the WITH* projections make no sense.
    if ((sp.storeKey.length || sp.storeDistKey.length)
            && (sp.withCoord || sp.withDist || sp.withHash))
    {
        repError(o, "ERR STORE option in GEORADIUS is not compatible with "
                ~ "WITHDIST, WITHHASH and WITHCOORD options");
        return;
    }
    GeoHit[] hits;
    bool wrong;
    auto r = runSearch(ks, sp, arena, hits, wrong);
    if (wrong)
    {
        repWrongTypeGeo(o);
        return;
    }
    if (r < 0)
    {
        repNoMember(o, sp.member);
        return;
    }
    if (sp.storeKey.length)
        storeHits(ks, sp.storeKey, hits, false, sp.unit, o);
    else if (sp.storeDistKey.length)
        storeHits(ks, sp.storeDistKey, hits, true, sp.unit, o);
    else
        emitHits(o, hits, sp);
}

// ---------------------------------------------------------------------------
// small reply helpers
// ---------------------------------------------------------------------------

/// Redis prints coordinates with 17 significant digits.
// WITHCOORD coordinate formatting. Valkey prints the decoded cell-center as a long double
// with ld2string(LD_STR_HUMAN) ("%.17Lf" + trailing-zero strip), e.g. -73.97334784269332886.
// dreads deliberately emits the shorter "%.17g" of the same double instead: the decoded
// value is a plain double and the extra digits Valkey prints (decimal places ~16-20) are
// pure decode noise of a 52-bit geohash — physically meaningless (sub-micron). Matching
// them byte-for-byte would emit ~2 more chars per coordinate, and GEORADIUS WITHCOORD is
// reply-bound: a +6% reply measured a proportional ~6% throughput drop. "%.17g" round-trips
// the double exactly, so no precision that matters is lost. (Valkey's own output is anyway
// platform-dependent here: on ARM, long double == double and it prints the short form too.)
private void repCoord(ref ByteBuffer o, double v) @nogc nothrow
{
    char[40] b = void;
    auto n = snprintf(b.ptr, b.length, "%.17g", v);
    repBulk(o, b[0 .. n]);
}

private void repWrongTypeGeo(ref ByteBuffer o) @nogc nothrow
{
    repError(o, "WRONGTYPE Operation against a key holding the wrong kind of value");
}
