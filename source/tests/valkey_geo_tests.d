module tests.valkey_geo_tests;

// Native in-process port of Valkey unit/geo.tcl (read-only spec + oracle; no tcl
// ships). Expected reply bytes grounded against valkey-server on port 7523.
// Deterministic scenarios only: GEOADD counts/options, GEOHASH strings, GEODIST
// fixed values, GEOSEARCH/GEORADIUS ordered/unordered members, WITHDIST/WITHHASH
// projections, STORE/STOREDIST, error + wrong-type + missing paths. Coordinate
// (WITHCOORD) exact float bytes and the fuzzy loops stay out; BYPOLYGON is a
// Valkey-only extension dreads does not parse, so it is skipped.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith, indexOf;
    import std.algorithm : sort;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] a...)
    {
        string r = "*" ~ a.length.to!string ~ "\r\n";
        foreach (x; a)
            r ~= "$" ~ x.length.to!string ~ "\r\n" ~ x ~ "\r\n";
        return r;
    }

    private string run(ref Keyspace ks, string[] c...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t p = 0;
        propagationOverride.clear();
        auto e = c.respCmd;
        parseValue(cast(const(ubyte)[]) e, p, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena, 1_700_000_000_000UL);
        return (cast(string) o.data).idup;
    }

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    private string arrB(string[] it...)
    {
        string r = "*" ~ it.length.to!string ~ "\r\n";
        foreach (x; it)
            r ~= bulk(x);
        return r;
    }

    private string[] parseArr(string s)
    {
        string[] r;
        if (s.length == 0 || s[0] != '*')
            return r;
        immutable nl = s.indexOf("\r\n");
        immutable n = s[1 .. nl].to!int;
        size_t i = nl + 2;
        foreach (_; 0 .. n)
        {
            if (s[i] != '$')
                break;
            immutable ee = s[i .. $].indexOf("\r\n") + i;
            immutable ln = s[i + 1 .. ee].to!int;
            i = ee + 2;
            if (ln < 0)
            {
                r ~= null;
                continue;
            }
            r ~= s[i .. i + ln];
            i += ln + 2;
        }
        return r;
    }

    private void sameSet(string reply, string[] exp...)
    {
        auto g = parseArr(reply);
        sort(g);
        auto e = exp.dup;
        sort(e);
        g.expect.to.equal(e);
    }

    enum NIL = "$-1\r\n";

    // ---------------------------------------------------------------------
    // GEOADD: create / update / CH / NX / XX / option-combo errors
    // ---------------------------------------------------------------------
    @("valkey.geo.geoadd_options")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // create returns 1 (new member), re-add same returns 0 (update, no add)
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market")
            .expect.to.equal(":1\r\n");
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market")
            .expect.to.equal(":0\r\n");

        // CH counts a changed member (position moved) as 1
        ks.run("GEOADD", "nyc", "CH", "-73.9454966", "40.747534", "lic market")
            .expect.to.equal(":1\r\n");

        // NX on an existing member does not update -> 0 added
        ks.run("GEOADD", "nyc", "NX", "-73.9454966", "40.747533", "lic market")
            .expect.to.equal(":0\r\n");

        // XX only updates existing members (returns 0 added). Existing member ->
        // moves it; a member that doesn't exist is silently skipped.
        ks.run("GEOADD", "nyc", "XX", "-83.9454966", "40.747533", "lic market")
            .expect.to.equal(":0\r\n");
        ks.run("GEOADD", "nyc", "XX", "-80", "40", "nomember")
            .expect.to.equal(":0\r\n");

        // CH NX on existing -> 0 (NX blocks, nothing changed)
        ks.run("GEOADD", "nyc", "CH", "NX", "-73.9454966", "40.747533", "lic market")
            .expect.to.equal(":0\r\n");

        // XX + NX together is a syntax error
        ks.run("GEOADD", "nyc", "xx", "nx", "-73.9454966", "40.747533", "lic market")
            .startsWith("-ERR").expect.to.equal(true);

        // an unknown token in the option slot is a syntax error
        ks.run("GEOADD", "nyc", "ch", "xx", "foo", "-73.9", "40.7", "lic market")
            .startsWith("-ERR").expect.to.equal(true);

        // invalid (non-numeric) coordinates -> not a valid float
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market",
                "foo", "bar", "luck market")
            .startsWith("-ERR").expect.to.equal(true);
    }

    @("valkey.geo.geoadd_multi")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // multi-add returns count of *new* members
        ks.run("GEOADD", "nyc",
                "-73.9733487", "40.7648057", "central park n/q/r",
                "-73.9903085", "40.7362513", "union square",
                "-74.0131604", "40.7126674", "wtc one",
                "-73.7858139", "40.6428986", "jfk",
                "-73.9375699", "40.7498929", "q4",
                "-73.9564142", "40.7480973", "4545")
            .expect.to.equal(":6\r\n");
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market")
            .expect.to.equal(":1\r\n");

        // geoset scores are 52-bit geohash integers; check them exactly (oracle)
        ks.run("ZRANGE", "nyc", "0", "-1", "withscores")
            .expect.to.equal(arrB(
                "wtc one", "1791873972053020",
                "union square", "1791875485187452",
                "central park n/q/r", "1791875761332224",
                "4545", "1791875796750882",
                "lic market", "1791875804419201",
                "q4", "1791875830079666",
                "jfk", "1791895905559723"));
    }

    // ---------------------------------------------------------------------
    // GEOHASH / GEOPOS / GEODIST
    // ---------------------------------------------------------------------
    @("valkey.geo.hash_pos_dist")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GEOHASH — the 11-char base32 geohash (Wikipedia example)
        ks.run("GEOADD", "points", "-5.6", "42.6", "test");
        ks.run("GEOHASH", "points", "test").expect.to.equal(arrB("ezs42e44yx0"));

        // multiple members
        ks.run("DEL", "points");
        ks.run("GEOADD", "points", "10", "20", "a", "30", "40", "b");
        ks.run("GEOHASH", "points", "a", "b")
            .expect.to.equal(arrB("s5x1g8cu2y0", "sxj7d9v2fs0"));
        // missing member -> nil element
        ks.run("GEOHASH", "points", "missing").expect.to.equal("*1\r\n" ~ NIL);
        // only the key -> empty array
        ks.run("GEOHASH", "points").expect.to.equal("*0\r\n");

        // GEOPOS: outer array of one 2-tuple per member; each present member is a
        // 2-element array of %.17g bulk coords (the inner "*2\r\n" header follows).
        auto pos = ks.run("GEOPOS", "points", "a", "b");
        pos.startsWith("*2\r\n*2\r\n").expect.to.equal(true);
        // GEOPOS with a missing element in the middle -> that slot is a nil array
        // (*-1\r\n), present slots keep their 2-tuple. Ends with the nil-array 'b'? no:
        // order is a (present), x (missing -> *-1), b (present).
        auto posMiss = ks.run("GEOPOS", "points", "a", "x", "b");
        posMiss.startsWith("*3\r\n").expect.to.equal(true);
        (posMiss.indexOf("*-1\r\n") > 0).expect.to.equal(true);
        // only the key -> empty array
        ks.run("GEOPOS", "points").expect.to.equal("*0\r\n");

        // GEODIST fixed Palermo<->Catania in m and km (oracle bytes)
        ks.run("DEL", "sic");
        ks.run("GEOADD", "sic", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");
        ks.run("GEODIST", "sic", "Palermo", "Catania").expect.to.equal(bulk("166274.1516"));
        ks.run("GEODIST", "sic", "Palermo", "Catania", "km").expect.to.equal(bulk("166.2742"));
        // distance to self is 0.0000
        ks.run("GEODIST", "sic", "Palermo", "Palermo").expect.to.equal(bulk("0.0000"));
        // missing member -> nil bulk; missing key -> nil bulk
        ks.run("GEODIST", "sic", "Palermo", "Agrigento").expect.to.equal(NIL);
        ks.run("GEODIST", "sic", "Ragusa", "Agrigento").expect.to.equal(NIL);
        ks.run("GEODIST", "empty_key", "Palermo", "Catania").expect.to.equal(NIL);
        // unsupported unit
        ks.run("GEODIST", "sic", "Palermo", "Catania", "parsecs")
            .startsWith("-ERR").expect.to.equal(true);
    }

    // ---------------------------------------------------------------------
    // GEORADIUS / GEORADIUS_RO — sorted, withdist, count, ANY
    // ---------------------------------------------------------------------
    @("valkey.geo.georadius")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("GEOADD", "nyc",
                "-73.9733487", "40.7648057", "central park n/q/r",
                "-73.9903085", "40.7362513", "union square",
                "-74.0131604", "40.7126674", "wtc one",
                "-73.7858139", "40.6428986", "jfk",
                "-73.9375699", "40.7498929", "q4",
                "-73.9564142", "40.7480973", "4545",
                "-73.9454966", "40.747533", "lic market");

        // simple ASC ordering
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "3", "km", "asc")
            .expect.to.equal(arrB("central park n/q/r", "4545", "union square"));
        // _RO variant identical
        ks.run("GEORADIUS_RO", "nyc", "-73.9798091", "40.7598464", "3", "km", "asc")
            .expect.to.equal(arrB("central park n/q/r", "4545", "union square"));

        // WITHDIST ASC -> [member, %.4f dist] tuples
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "3", "km", "withdist", "asc")
            .expect.to.equal("*3\r\n"
                ~ "*2\r\n" ~ bulk("central park n/q/r") ~ bulk("0.7750")
                ~ "*2\r\n" ~ bulk("4545") ~ bulk("2.3651")
                ~ "*2\r\n" ~ bulk("union square") ~ bulk("2.7697"));

        // COUNT 3 (default sorted by distance)
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "3")
            .expect.to.equal(arrB("central park n/q/r", "4545", "union square"));

        // COUNT 2 DESC
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT", "2", "DESC")
            .expect.to.equal(arrB("wtc one", "q4"));

        // COUNT n ANY ASC — ANY picks any n then ASC sorts them
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km",
                "COUNT", "3", "ANY", "ASC")
            .expect.to.equal(arrB("central park n/q/r", "union square", "wtc one"));

        // WITHDIST WITHHASH WITHCOORD COUNT 2 — hash int + %.4f dist deterministic.
        // Coords: dreads emits "%.17g" of the decoded double (NOT Valkey's longer
        // "%.17Lf" long-double form). The differing tail digits are 52-bit-geohash
        // decode noise (sub-micron, meaningless) and matching them costs ~6% on this
        // reply-bound path; "%.17g" round-trips the double exactly. See repCoord in geo.d.
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km",
                "WITHDIST", "WITHHASH", "WITHCOORD", "COUNT", "2")
            .expect.to.equal("*2\r\n"
                ~ "*4\r\n" ~ bulk("central park n/q/r") ~ bulk("0.7750")
                    ~ ":1791875761332224\r\n"
                    ~ "*2\r\n" ~ bulk("-73.973347842693329") ~ bulk("40.76480639569882")
                ~ "*4\r\n" ~ bulk("4545") ~ bulk("2.3651")
                    ~ ":1791875796750882\r\n"
                    ~ "*2\r\n" ~ bulk("-73.956412374973297") ~ bulk("40.748097513816454"));

        // ANY without COUNT is an error
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "ANY", "ASC")
            .startsWith("-ERR").expect.to.equal(true);
        // COUNT without an integer argument is a syntax error
        ks.run("GEORADIUS", "nyc", "-73.9798091", "40.7598464", "10", "km", "COUNT")
            .startsWith("-ERR").expect.to.equal(true);
    }

    @("valkey.geo.georadius_huge")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // issue #2767 — a 50000 km radius must still return the single member.
        // WITHCOORD makes each element a nested array, so just check the outer
        // count (one hit) and that the member name is present.
        ks.run("GEOADD", "users", "-47.271613776683807", "-54.534504198047678", "user_000000");
        auto r = ks.run("GEORADIUS", "users", "0", "0", "50000", "km", "WITHCOORD");
        r.startsWith("*1\r\n").expect.to.equal(true);
        (r.indexOf("user_000000") > 0).expect.to.equal(true);
        // plain form (no WITH*) is a flat array of one member
        ks.run("GEORADIUS", "users", "0", "0", "50000", "km").expect.to.equal(arrB("user_000000"));
    }

    // ---------------------------------------------------------------------
    // GEORADIUSBYMEMBER / _RO
    // ---------------------------------------------------------------------
    @("valkey.geo.georadiusbymember")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("GEOADD", "nyc",
                "-73.9733487", "40.7648057", "central park n/q/r",
                "-73.9903085", "40.7362513", "union square",
                "-74.0131604", "40.7126674", "wtc one",
                "-73.7858139", "40.6428986", "jfk",
                "-73.9375699", "40.7498929", "q4",
                "-73.9564142", "40.7480973", "4545",
                "-73.9454966", "40.747533", "lic market");

        ks.run("GEORADIUSBYMEMBER", "nyc", "wtc one", "7", "km")
            .expect.to.equal(arrB("wtc one", "union square",
                "central park n/q/r", "4545", "lic market"));
        ks.run("GEORADIUSBYMEMBER_RO", "nyc", "wtc one", "7", "km")
            .expect.to.equal(arrB("wtc one", "union square",
                "central park n/q/r", "4545", "lic market"));

        // WITHDIST from a member: the member itself is 0.0000
        ks.run("GEORADIUSBYMEMBER", "nyc", "wtc one", "7", "km", "withdist")
            .expect.to.equal("*5\r\n"
                ~ "*2\r\n" ~ bulk("wtc one") ~ bulk("0.0000")
                ~ "*2\r\n" ~ bulk("union square") ~ bulk("3.2544")
                ~ "*2\r\n" ~ bulk("central park n/q/r") ~ bulk("6.7000")
                ~ "*2\r\n" ~ bulk("4545") ~ bulk("6.1975")
                ~ "*2\r\n" ~ bulk("lic market") ~ bulk("6.8969"));

        // member that does not exist -> specific error
        ks.run("DEL", "Sicily");
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");
        ks.run("GEORADIUSBYMEMBER", "Sicily", "none_exist_member", "300", "KM")
            .startsWith("-ERR member none_exist_member does not exist").expect.to.equal(true);
    }

    @("valkey.geo.bymember_oblique_pole")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // satisfied points in oblique direction near the pole
        ks.run("GEOADD", "k1", "-0.15307903289794921875", "85", "n1",
                "0.3515625", "85.00019260486917005437", "n2");
        sameSet(ks.run("GEORADIUSBYMEMBER", "k1", "n1", "4891.94", "m"), "n1", "n2");

        ks.run("ZREM", "k1", "n1", "n2");
        ks.run("GEOADD", "k1", "-4.95211958885192871094", "85", "n3", "11.25", "85.0511", "n4");
        sameSet(ks.run("GEORADIUSBYMEMBER", "k1", "n3", "156544", "m"), "n3", "n4");

        ks.run("ZREM", "k1", "n3", "n4");
        ks.run("GEOADD", "k1", "-45", "65.50900022111811438208", "n5", "90", "85.0511", "n6");
        sameSet(ks.run("GEORADIUSBYMEMBER", "k1", "n5", "5009431", "m"), "n5", "n6");

        // crossing-pole search
        ks.run("DEL", "k1");
        ks.run("GEOADD", "k1", "45", "65", "n1", "-135", "85.05", "n2");
        sameSet(ks.run("GEORADIUSBYMEMBER", "k1", "n1", "5009431", "m"), "n1", "n2");
    }

    // ---------------------------------------------------------------------
    // GEOSEARCH — origin/shape validation + ordered results
    // ---------------------------------------------------------------------
    @("valkey.geo.geosearch")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("GEOADD", "nyc",
                "-73.9733487", "40.7648057", "central park n/q/r",
                "-73.9903085", "40.7362513", "union square",
                "-74.0131604", "40.7126674", "wtc one",
                "-73.7858139", "40.6428986", "jfk",
                "-73.9375699", "40.7498929", "q4",
                "-73.9564142", "40.7480973", "4545",
                "-73.9454966", "40.747533", "lic market");

        // FROMLONLAT BYBOX ASC
        ks.run("GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464",
                "bybox", "6", "6", "km", "asc")
            .expect.to.equal(arrB("central park n/q/r", "4545", "union square", "lic market"));

        // FROMLONLAT BYBOX WITHDIST ASC
        ks.run("GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464",
                "bybox", "6", "6", "km", "withdist", "asc")
            .expect.to.equal("*4\r\n"
                ~ "*2\r\n" ~ bulk("central park n/q/r") ~ bulk("0.7750")
                ~ "*2\r\n" ~ bulk("4545") ~ bulk("2.3651")
                ~ "*2\r\n" ~ bulk("union square") ~ bulk("2.7697")
                ~ "*2\r\n" ~ bulk("lic market") ~ bulk("3.1991"));

        // FROMMEMBER BYBOX
        ks.run("GEOSEARCH", "nyc", "frommember", "wtc one", "bybox", "14", "14", "km")
            .expect.to.equal(arrB("wtc one", "union square",
                "central park n/q/r", "4545", "lic market", "q4"));

        // FROMMEMBER on a non-existing member -> error
        ks.run("DEL", "Sicily");
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");
        ks.run("GEOSEARCH", "Sicily", "FROMMEMBER", "none_exist_member", "BYRADIUS", "300", "KM")
            .startsWith("-ERR member none_exist_member does not exist").expect.to.equal(true);

        // FROMLONLAT and FROMMEMBER cannot both be present
        ks.run("GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464",
                "frommember", "xxx", "bybox", "6", "6", "km", "asc")
            .startsWith("-ERR").expect.to.equal(true);
        // one origin must exist
        ks.run("GEOSEARCH", "nyc", "bybox", "3", "3", "km", "asc", "desc",
                "withhash", "withdist", "withcoord")
            .startsWith("-ERR").expect.to.equal(true);
        // BYRADIUS and BYBOX cannot both be present
        ks.run("GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464",
                "byradius", "3", "km", "bybox", "3", "3", "km", "asc")
            .startsWith("-ERR").expect.to.equal(true);
        // STOREDIST is not a GEOSEARCH option (only GEOSEARCHSTORE) -> syntax error
        ks.run("GEOSEARCH", "nyc", "fromlonlat", "-73.9798091", "40.7598464",
                "bybox", "6", "6", "km", "asc", "storedist")
            .startsWith("-ERR").expect.to.equal(true);
    }

    @("valkey.geo.geosearch_geometry")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GEOSEARCH vs GEORADIUS: box widens the candidate set
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania",
                "12.758489", "38.788135", "edge1",
                "17.241510", "38.788135", "eage2");
        ks.run("GEORADIUS", "Sicily", "15", "37", "200", "km", "asc")
            .expect.to.equal(arrB("Catania", "Palermo"));
        ks.run("GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "400", "400", "km", "asc")
            .expect.to.equal(arrB("Catania", "Palermo", "eage2", "edge1"));

        // non-square, long and narrow box -> only test1
        ks.run("DEL", "Sicily");
        ks.run("GEOADD", "Sicily", "12.75", "36.995", "test1");
        ks.run("GEOADD", "Sicily", "12.75", "36.50", "test2");
        ks.run("GEOADD", "Sicily", "13.00", "36.50", "test3");
        ks.run("GEOSEARCH", "Sicily", "fromlonlat", "15", "37", "bybox", "400", "2", "km")
            .expect.to.equal(arrB("test1"));

        // corner-point box: 4 edge members are the corners of a 400x400 box (order
        // is scan-dependent, so compare as a set)
        ks.run("DEL", "Sicily");
        ks.run("GEOADD", "Sicily",
                "12.758489", "38.788135", "edge1",
                "17.241510", "38.788135", "edge2",
                "17.250000", "35.202000", "edge3",
                "12.750000", "35.202000", "edge4",
                "12.748489955781654", "37", "edge5",
                "15", "38.798135872540925", "edge6",
                "17.251510044218346", "37", "edge7",
                "15", "35.201864127459075", "edge8");
        sameSet(ks.run("GEOSEARCH", "Sicily", "fromlonlat", "15", "37",
                "bybox", "400", "400", "km", "asc"),
            "edge1", "edge2", "edge5", "edge7");

        // box that spans the antimeridian (+/-180 deg)
        ks.run("DEL", "points");
        ks.run("GEOADD", "points", "179.5", "36", "point1");
        ks.run("GEOADD", "points", "-179.5", "36", "point2");
        ks.run("GEOSEARCH", "points", "fromlonlat", "179", "37", "bybox", "400", "400", "km", "asc")
            .expect.to.equal(arrB("point1", "point2"));
        ks.run("GEOSEARCH", "points", "fromlonlat", "-179", "37", "bybox", "400", "400", "km", "asc")
            .expect.to.equal(arrB("point2", "point1"));

        // small-distance WITHDIST in miles
        ks.run("DEL", "pts2");
        ks.run("GEOADD", "pts2", "-122.407107", "37.794300", "1");
        ks.run("GEOADD", "pts2", "-122.227336", "37.794300", "2");
        ks.run("GEORADIUS", "pts2", "-122.407107", "37.794300", "30", "mi", "ASC", "WITHDIST")
            .expect.to.equal("*2\r\n"
                ~ "*2\r\n" ~ bulk("1") ~ bulk("0.0001")
                ~ "*2\r\n" ~ bulk("2") ~ bulk("9.8182"));

        // exact zero distance for a full-precision coincident point
        ks.run("DEL", "pts3");
        ks.run("GEOADD", "pts3", "-122.40710645914077759", "37.79430076631935975", "position");
        ks.run("GEOSEARCH", "pts3", "FROMMEMBER", "position", "BYRADIUS", "0", "mi", "ASC", "WITHDIST")
            .expect.to.equal("*1\r\n" ~ "*2\r\n" ~ bulk("position") ~ bulk("0.0000"));
        ks.run("GEOSEARCH", "pts3", "FROMLONLAT", "-122.40710645914077759",
                "37.79430076631935975", "BYRADIUS", "0", "mi", "ASC", "WITHDIST")
            .expect.to.equal("*1\r\n" ~ "*2\r\n" ~ bulk("position") ~ bulk("0.0000"));
    }

    // ---------------------------------------------------------------------
    // Wrong-type / non-existing / empty-search edge responses
    // ---------------------------------------------------------------------
    @("valkey.geo.edge_responses")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // wrong-type source key -> WRONGTYPE on every read/store form
        ks.run("SET", "src", "wrong_type").expect.to.equal("+OK\r\n");
        ks.run("GEORADIUS", "src", "1", "1", "1", "km")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEORADIUS", "src", "1", "1", "1", "km", "store", "dest")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEOSEARCH", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEOSEARCHSTORE", "dest", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEORADIUSBYMEMBER", "src", "member", "1", "km")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEODIST", "src", "member", "1")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEOHASH", "src", "member")
            .startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("GEOPOS", "src", "member")
            .startsWith("-WRONGTYPE").expect.to.equal(true);

        // non-existing source key -> empty array (read) / 0 (store)
        ks.run("DEL", "src");
        ks.run("GEORADIUS", "src", "1", "1", "1", "km").expect.to.equal("*0\r\n");
        ks.run("GEORADIUS", "src", "1", "1", "1", "km", "store", "dest")
            .expect.to.equal(":0\r\n");
        ks.run("GEOSEARCH", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
            .expect.to.equal("*0\r\n");
        ks.run("GEOSEARCHSTORE", "dest", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
            .expect.to.equal(":0\r\n");

        // populated key but the search area is empty -> empty array / 0-store
        ks.run("GEOADD", "src", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");
        ks.run("GEORADIUS", "src", "1", "1", "1", "km").expect.to.equal("*0\r\n");
        ks.run("GEORADIUS", "src", "1", "1", "1", "km", "store", "dest")
            .expect.to.equal(":0\r\n");
        ks.run("GEOSEARCH", "src", "fromlonlat", "0", "0", "byradius", "1", "km")
            .expect.to.equal("*0\r\n");
    }

    // ---------------------------------------------------------------------
    // STORE / STOREDIST
    // ---------------------------------------------------------------------
    @("valkey.geo.store")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("GEOADD", "pts", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");

        // STORE syntax error: missing destination key
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "50", "km", "store")
            .startsWith("-ERR").expect.to.equal(true);
        // GEOSEARCHSTORE with a stray STORE token is a syntax error
        ks.run("GEOSEARCHSTORE", "abc", "pts", "fromlonlat", "13.361389", "38.115556",
                "byradius", "50", "km", "store", "abc")
            .startsWith("-ERR").expect.to.equal(true);

        // STORE is incompatible with WITH* projections
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "50", "km", "store", "pts2", "withdist")
            .startsWith("-ERR").expect.to.equal(true);
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "50", "km", "store", "pts2", "withhash")
            .startsWith("-ERR").expect.to.equal(true);
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "50", "km", "store", "pts2", "withcoord")
            .startsWith("-ERR").expect.to.equal(true);

        // STORE plain: destination gets the same members (geohash-int scores)
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "500", "km", "store", "pts2");
        ks.run("ZRANGE", "pts2", "0", "-1").expect.to.equal(arrB("Palermo", "Catania"));

        // GEOSEARCHSTORE plain usage mirrors the source ordering
        ks.run("GEOSEARCHSTORE", "pts3", "pts", "fromlonlat", "13.361389", "38.115556",
                "byradius", "500", "km");
        ks.run("ZRANGE", "pts3", "0", "-1").expect.to.equal(arrB("Palermo", "Catania"));

        // GEORADIUSBYMEMBER STORE / STOREDIST
        ks.run("GEORADIUSBYMEMBER", "pts", "Palermo", "500", "km", "store", "pts4");
        ks.run("ZRANGE", "pts4", "0", "-1").expect.to.equal(arrB("Palermo", "Catania"));
        // STOREDIST orders by distance (Catania is origin -> 0)
        ks.run("GEORADIUSBYMEMBER", "pts", "Catania", "500", "km", "storedist", "pts5");
        ks.run("ZRANGE", "pts5", "0", "-1").expect.to.equal(arrB("Catania", "Palermo"));

        // STOREDIST with COUNT + ASC keeps the nearest single member
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "500", "km",
                "storedist", "pts6", "asc", "count", "1");
        ks.run("ZCARD", "pts6").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "pts6", "0", "-1").expect.to.equal(arrB("Palermo"));
        // STOREDIST with COUNT + DESC keeps the farthest single member
        ks.run("GEORADIUS", "pts", "13.361389", "38.115556", "500", "km",
                "storedist", "pts7", "desc", "count", "1");
        ks.run("ZCARD", "pts7").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "pts7", "0", "-1").expect.to.equal(arrB("Catania"));
    }
}
