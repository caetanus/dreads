module tests.valkey_geo_tests;

// Valkey unit/geo.tcl core ported to native in-process UT (see valkey_incr_tests.d
// for rationale + THIRD_PARTY_NOTICES credit). Deterministic parts: GEOADD counts,
// GEOHASH strings, GEODIST (fixed value), GEOSEARCH ordered members. GEOPOS exact
// float round-trips (tolerance-based) stay in the blackbox sweep.

version (unittest)
{
    import fluent.asserts;
    import std.conv : to;

    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] args...)
    {
        string r = "*" ~ args.length.to!string ~ "\r\n";
        foreach (a; args)
            r ~= "$" ~ a.length.to!string ~ "\r\n" ~ a ~ "\r\n";
        return r;
    }

    private string run(ref Keyspace ks, string[] cmdArgs...)
    {
        Arena arena;
        ByteBuffer o;
        RVal v;
        size_t pos = 0;
        propagationOverride.clear();
        auto encoded = cmdArgs.respCmd;
        parseValue(cast(const(ubyte)[]) encoded, pos, arena, v).expect.to.equal(ParseStatus.ok);
        v.dispatch(ks, o, arena);
        return (cast(string) o.data).idup;
    }

    private string bulk(string p)
    {
        return "$" ~ p.length.to!string ~ "\r\n" ~ p ~ "\r\n";
    }

    private string arrB(string[] items...)
    {
        string r = "*" ~ items.length.to!string ~ "\r\n";
        foreach (it; items)
            r ~= bulk(it);
        return r;
    }

    @("valkey.geo.add_hash_dist")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        // GEOADD create/update
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market").expect.to.equal(":1\r\n");
        ks.run("GEOADD", "nyc", "-73.9454966", "40.747533", "lic market").expect.to.equal(":0\r\n"); // update
        // CH option counts changed members
        ks.run("GEOADD", "nyc", "CH", "-73.9454966", "40.747534", "lic market").expect.to.equal(":1\r\n");

        // GEOHASH — deterministic 11-char geohash (Wikipedia example)
        ks.run("GEOADD", "points", "-5.6", "42.6", "test");
        ks.run("GEOHASH", "points", "test").expect.to.equal(arrB("ezs42e44yx0"));
        ks.run("GEOHASH", "points", "missing").expect.to.equal("*1\r\n$-1\r\n"); // nil for absent

        // GEODIST Palermo<->Catania: fixed values in m and km
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");
        ks.run("GEODIST", "Sicily", "Palermo", "Catania").expect.to.equal(bulk("166274.1516"));
        ks.run("GEODIST", "Sicily", "Palermo", "Catania", "km").expect.to.equal(bulk("166.2742"));
        ks.run("GEODIST", "Sicily", "Palermo", "missing").expect.to.equal("$-1\r\n");
    }

    @("valkey.geo.search")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();

        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania",
                "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2");

        // GEOSEARCH FROMLONLAT ... BYRADIUS ... ASC -> members nearest-first
        ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
            .expect.to.equal(arrB("Catania", "Palermo"));
        // wider radius pulls the edges in too
        ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "400", "km", "ASC")
            .expect.to.equal(arrB("Catania", "Palermo", "edge2", "edge1"));

        // FROMMEMBER: Palermo (self, 0), edge1 (~90km), Catania (166km) all within 200km
        ks.run("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC")
            .expect.to.equal(arrB("Palermo", "edge1", "Catania"));
    }
}
