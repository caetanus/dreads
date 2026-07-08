module tests.geo_tests;

// GEO command suite, using the fixtures from the official Redis docs:
// Palermo (13.361389, 38.115556) and Catania (15.087269, 37.502669).

version (unittest)
{
    import fluent.asserts;

    import dreads.commands;
    import dreads.geo : geohashDecode, geohashEncode, haversine;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;

    private string respCmd(string[] args...)
    {
        import std.conv : to;

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

    @("geo.encode_roundtrip")
    unittest
    {
        double lon, lat;
        geohashDecode(geohashEncode(13.361389, 38.115556), lon, lat);
        (lon > 13.3613 && lon < 13.3615).expect.to.equal(true);
        (lat > 38.1155 && lat < 38.1156).expect.to.equal(true);
        // haversine sanity: Palermo-Catania ~166274m (Redis's exact figure)
        auto d = haversine(13.361389, 38.115556, 15.087269, 37.502669);
        (d > 166_270 && d < 166_280).expect.to.equal(true);
    }

    @("geo.add_pos_dist_hash")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania").expect.to.equal(":2\r\n");
        ks.run("TYPE", "Sicily").expect.to.equal("+zset\r\n"); // geo keys ARE zsets
        ks.run("ZCARD", "Sicily").expect.to.equal(":2\r\n");

        // NX/XX/CH flags
        ks.run("GEOADD", "Sicily", "NX", "1", "1", "Palermo").expect.to.equal(":0\r\n");
        ks.run("GEOADD", "Sicily", "XX", "CH", "13.5", "38.2", "Palermo").expect.to.equal(":1\r\n");
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"); // restore

        auto pos = ks.run("GEOPOS", "Sicily", "Palermo", "ghost");
        pos.expect.to.contain("13.3613893"); // decoded cell center
        pos.expect.to.contain("38.1155563");
        pos.expect.to.contain("*-1\r\n"); // missing member is a nil array

        ks.run("GEODIST", "Sicily", "Palermo", "Catania")
            .expect.to.equal("$11\r\n166274.1516\r\n");
        ks.run("GEODIST", "Sicily", "Palermo", "Catania", "km")
            .expect.to.equal("$8\r\n166.2742\r\n");
        ks.run("GEODIST", "Sicily", "Palermo", "ghost").expect.to.equal("$-1\r\n");
        ks.run("GEODIST", "Sicily", "a", "b", "parsec")[0].expect.to.equal('-');

        auto gh = ks.run("GEOHASH", "Sicily", "Palermo", "Catania");
        gh.expect.to.contain("sqc8b49rny0"); // Redis's documented outputs
        gh.expect.to.contain("sqdtr74hyu0"); // (last char padded with zero bits)

        ks.run("GEOADD", "Sicily", "200", "10", "bad")[0].expect.to.equal('-'); // bad lon
    }

    @("geo.search")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania", "12.758489", "38.788135", "edge");

        // radius from a point: 200km around (15, 37) hits Catania only
        ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "100",
                "km", "ASC").expect.to.equal("*1\r\n$7\r\nCatania\r\n");
        // wider radius, ordered by distance ("edge" sits ~280km away)
        auto both = ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
                "BYRADIUS", "300", "km", "ASC");
        both[0 .. 4].expect.to.equal("*3\r\n");
        // FROMMEMBER
        ks.run("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo", "BYRADIUS", "1", "km")
            .expect.to.equal("*1\r\n$7\r\nPalermo\r\n");
        ks.run("GEOSEARCH", "Sicily", "FROMMEMBER", "ghost", "BYRADIUS", "1", "km")[0]
            .expect.to.equal('-');
        // BYBOX 400x400km around the middle catches Palermo and Catania
        auto boxed = ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "14.2", "37.8",
                "BYBOX", "400", "400", "km", "ASC");
        boxed.expect.to.contain("Palermo");
        boxed.expect.to.contain("Catania");
        // COUNT limits
        ks.run("GEOSEARCH", "Sicily", "FROMLONLAT", "14.2", "37.8", "BYRADIUS",
                "500", "km", "ASC", "COUNT", "1")[0 .. 4].expect.to.equal("*1\r\n");
        // WITHDIST/WITHCOORD shape
        auto rich = ks.run("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo",
                "BYRADIUS", "1", "km", "WITHDIST", "WITHCOORD");
        rich.expect.to.contain("0.0000");
        rich.expect.to.contain("13.3613893");
        // missing key -> empty
        ks.run("GEOSEARCH", "nada", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km")
            .expect.to.equal("*0\r\n");
    }

    @("geo.searchstore_and_radius")
    unittest
    {
        Keyspace ks;
        scope (exit)
            ks.d.free();
        ks.run("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                "15.087269", "37.502669", "Catania");

        ks.run("GEOSEARCHSTORE", "near", "Sicily", "FROMLONLAT", "15", "37",
                "BYRADIUS", "100", "km").expect.to.equal(":1\r\n");
        ks.run("ZRANGE", "near", "0", "-1").expect.to.equal("*1\r\n$7\r\nCatania\r\n");
        // STOREDIST stores the distance as score (km here)
        ks.run("GEOSEARCHSTORE", "neard", "Sicily", "FROMLONLAT", "15", "37",
                "BYRADIUS", "100", "km", "STOREDIST").expect.to.equal(":1\r\n");
        auto score = ks.run("ZSCORE", "neard", "Catania");
        score.expect.to.contain("5"); // ~56km
        // empty result deletes the destination
        ks.run("GEOSEARCHSTORE", "near", "Sicily", "FROMLONLAT", "0", "0",
                "BYRADIUS", "1", "m").expect.to.equal(":0\r\n");
        ks.run("EXISTS", "near").expect.to.equal(":0\r\n");

        // legacy GEORADIUS forms map onto the same core
        ks.run("GEORADIUS", "Sicily", "15", "37", "200", "km")
            .expect.to.contain("Catania");
        ks.run("GEORADIUSBYMEMBER", "Sicily", "Palermo", "1", "km")
            .expect.to.equal("*1\r\n$7\r\nPalermo\r\n");
        ks.run("GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STORE", "x")[0]
            .expect.to.equal('-'); // no STORE on the _RO form
    }
}
