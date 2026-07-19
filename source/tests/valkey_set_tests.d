module tests.valkey_set_tests;

// Native in-process port of Valkey unit/type/set.tcl (read-only spec + reply oracle).
// Set replies are UNORDERED, so array results compare as sorted multisets (sameSet).
// Encoding-conversion duplicates (foreach encoding / *-max-* configs), DEBUG RELOAD,
// fuzzing, MULTI/WATCH, replication-stream and memory-usage cases are intentionally
// skipped -- the semantic is ported ONCE. See THIRD_PARTY_NOTICES for Valkey credit.
version (unittest) {
    import fluent.asserts;
    import std.conv : to;
    import std.string : startsWith, indexOf;
    import std.algorithm : sort;
    import dreads.commands;
    import dreads.mem : Arena, ByteBuffer;
    import dreads.obj : Keyspace;
    import dreads.resp;
    private string respCmd(string[] a...){ string r="*"~a.length.to!string~"\r\n"; foreach(x;a) r~="$"~x.length.to!string~"\r\n"~x~"\r\n"; return r; }
    private string run(ref Keyspace ks, string[] c...){ Arena arena; ByteBuffer o; RVal v; size_t p=0; propagationOverride.clear(); auto e=c.respCmd; parseValue(cast(const(ubyte)[])e,p,arena,v).expect.to.equal(ParseStatus.ok); v.dispatch(ks,o,arena,1_700_000_000_000UL); return (cast(string)o.data).idup; }
    private string bulk(string p){ return "$"~p.length.to!string~"\r\n"~p~"\r\n"; }
    private string arrB(string[] it...){ string r="*"~it.length.to!string~"\r\n"; foreach(x;it) r~=bulk(x); return r; }
    private string[] parseArr(string s){ string[] r; if(s.length==0||s[0]!='*') return r; immutable nl=s.indexOf("\r\n"); immutable n=s[1..nl].to!int; size_t i=nl+2; foreach(_;0..n){ if(s[i]!='$') break; immutable ee=s[i..$].indexOf("\r\n")+i; immutable ln=s[i+1..ee].to!int; i=ee+2; if(ln<0){ r~=null; continue; } r~=s[i..i+ln]; i+=ln+2; } return r; }
    private void sameSet(string reply, string[] exp...){ auto g=parseArr(reply); sort(g); auto e=exp.dup; sort(e); g.expect.to.equal(e); }
    enum NIL="$-1\r\n";

    // SADD / SCARD / SISMEMBER / SMISMEMBER / SMEMBERS basics (set.tcl:21-56)
    @("valkey.set.basics") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","myset","foo").expect.to.equal(":1\r\n");
        ks.run("SADD","myset","bar").expect.to.equal(":1\r\n"); // new member
        ks.run("SADD","myset","bar").expect.to.equal(":0\r\n"); // already present
        ks.run("SCARD","myset").expect.to.equal(":2\r\n");

        ks.run("SISMEMBER","myset","foo").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER","myset","bar").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER","myset","bla").expect.to.equal(":0\r\n");

        ks.run("SMISMEMBER","myset","foo").expect.to.equal("*1\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","foo","bar").expect.to.equal("*2\r\n:1\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","foo","bla").expect.to.equal("*2\r\n:1\r\n:0\r\n");
        ks.run("SMISMEMBER","myset","bla","foo").expect.to.equal("*2\r\n:0\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","bla").expect.to.equal("*1\r\n:0\r\n");

        sameSet(ks.run("SMEMBERS","myset"), "foo", "bar");
    }

    // intset flavor of the same basics (set.tcl:40-56) -- integer members
    @("valkey.set.basics_intset") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","myset","17").expect.to.equal(":1\r\n");
        ks.run("SADD","myset","16").expect.to.equal(":1\r\n");
        ks.run("SADD","myset","16").expect.to.equal(":0\r\n");
        ks.run("SCARD","myset").expect.to.equal(":2\r\n");
        ks.run("SISMEMBER","myset","16").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER","myset","17").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER","myset","18").expect.to.equal(":0\r\n");
        ks.run("SMISMEMBER","myset","16").expect.to.equal("*1\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","16","17").expect.to.equal("*2\r\n:1\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","16","18").expect.to.equal("*2\r\n:1\r\n:0\r\n");
        ks.run("SMISMEMBER","myset","18","16").expect.to.equal("*2\r\n:0\r\n:1\r\n");
        ks.run("SMISMEMBER","myset","18").expect.to.equal("*1\r\n:0\r\n");
        sameSet(ks.run("SMEMBERS","myset"), "16", "17");

        // an integer larger than 64 bits is kept as a string member (set.tcl:101-106)
        ks.run("SADD","big","213244124402402314402033402").expect.to.equal(":1\r\n");
        ks.run("SISMEMBER","big","213244124402402314402033402").expect.to.equal(":1\r\n");
        ks.run("SMISMEMBER","big","213244124402402314402033402").expect.to.equal("*1\r\n:1\r\n");
    }

    // wrong-type / non-existing-key handling (set.tcl:58-70, 81-84)
    @("valkey.set.wrongtype_and_missing") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        // SMISMEMBER / SMEMBERS / SCARD / SADD against a non-set -> WRONGTYPE
        ks.run("LPUSH","mylist","foo").expect.to.equal(":1\r\n");
        ks.run("SMISMEMBER","mylist","bar").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SMEMBERS","mylist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SCARD","mylist").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SADD","mylist","bar").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SISMEMBER","mylist","bar").startsWith("-WRONGTYPE").should.equal(true);

        // against a non-existing key
        ks.run("SMISMEMBER","myset1","foo").expect.to.equal("*1\r\n:0\r\n");
        ks.run("SMISMEMBER","myset1","foo","bar").expect.to.equal("*2\r\n:0\r\n:0\r\n");
        ks.run("SMEMBERS","myset1").expect.to.equal("*0\r\n");
        ks.run("SCARD","myset1").expect.to.equal(":0\r\n");
        ks.run("SISMEMBER","myset1","foo").expect.to.equal(":0\r\n");

        // SMISMEMBER requires one or more members (set.tcl:72-79)
        ks.run("SMISMEMBER","myset").startsWith("-ERR").should.equal(true);
    }

    // Variadic SADD counting (set.tcl:173-178)
    @("valkey.set.variadic_sadd") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("SADD","myset","a","b","c").expect.to.equal(":3\r\n");
        // only A and B are new; a b c already present
        ks.run("SADD","myset","A","a","b","c","B").expect.to.equal(":2\r\n");
        sameSet(ks.run("SMEMBERS","myset"), "A", "a", "b", "c", "B");
    }

    // SREM basics (set.tcl:202-232)
    @("valkey.set.srem") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","myset","foo","bar","ciao");
        ks.run("SREM","myset","qux").expect.to.equal(":0\r\n"); // not a member
        ks.run("SREM","myset","ciao").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","myset"), "foo", "bar");

        // intset flavor
        ks.run("SADD","iset","3","4","5");
        ks.run("SREM","iset","6").expect.to.equal(":0\r\n");
        ks.run("SREM","iset","4").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","iset"), "3", "5");

        // multiple arguments, only present ones count
        ks.run("SADD","m2","a","b","c","d");
        ks.run("SREM","m2","k","k","k").expect.to.equal(":0\r\n");
        ks.run("SREM","m2","b","d","x","y").expect.to.equal(":2\r\n");
        sameSet(ks.run("SMEMBERS","m2"), "a", "c");

        // variadic version with more args than needed to destroy the key
        ks.run("SADD","m3","1","2","3");
        ks.run("SREM","m3","1","2","3","4","5","6","7","8").expect.to.equal(":3\r\n");
        ks.run("EXISTS","m3").expect.to.equal(":0\r\n");

        // SREM on a non-existing key
        ks.run("SREM","nokey","a").expect.to.equal(":0\r\n");
    }

    // SINTERCARD argument validation + non-existing/wrong-type (set.tcl:234-266)
    @("valkey.set.sintercard_args") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SINTERCARD").startsWith("-ERR").should.equal(true);
        ks.run("SINTERCARD","1").startsWith("-ERR").should.equal(true);

        ks.run("SINTERCARD","0","myset").startsWith("-ERR numkeys").should.equal(true);
        ks.run("SINTERCARD","a","myset").startsWith("-ERR numkeys").should.equal(true);

        ks.run("SINTERCARD","2","myset").startsWith("-ERR Number of keys").should.equal(true);
        ks.run("SINTERCARD","3","myset","myset2").startsWith("-ERR Number of keys").should.equal(true);

        ks.run("SINTERCARD","1","myset","myset2").startsWith("-ERR syntax error").should.equal(true);
        ks.run("SINTERCARD","1","myset","bar_arg").startsWith("-ERR syntax error").should.equal(true);
        ks.run("SINTERCARD","1","myset","LIMIT").startsWith("-ERR syntax error").should.equal(true);

        ks.run("SINTERCARD","1","myset","LIMIT","-1").startsWith("-ERR LIMIT").should.equal(true);
        ks.run("SINTERCARD","1","myset","LIMIT","a").startsWith("-ERR LIMIT").should.equal(true);

        // against non-set -> WRONGTYPE
        ks.run("SADD","set","a","b","c");
        ks.run("SET","key1","x");
        ks.run("SINTERCARD","1","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SINTERCARD","2","set","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SINTERCARD","2","key1","noset").startsWith("-WRONGTYPE").should.equal(true);

        // against non-existing key -> 0 regardless of LIMIT
        ks.run("SINTERCARD","1","non-existing-key").expect.to.equal(":0\r\n");
        ks.run("SINTERCARD","1","non-existing-key","limit","0").expect.to.equal(":0\r\n");
        ks.run("SINTERCARD","1","non-existing-key","limit","10").expect.to.equal(":0\r\n");
    }

    // SINTER / SINTERCARD / SINTERSTORE across two and three sets (set.tcl:314-364)
    @("valkey.set.sinter") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","set1","195","196","197","198","199","foo");
        ks.run("SADD","set2","195","196","197","198","199","foo");
        ks.run("SADD","set3","199","195","1000","2000","foo");

        sameSet(ks.run("SINTER","set1","set2"), "195","196","197","198","199","foo");

        ks.run("SINTERCARD","2","set1","set2").expect.to.equal(":6\r\n");
        ks.run("SINTERCARD","2","set1","set2","limit","0").expect.to.equal(":6\r\n");
        ks.run("SINTERCARD","2","set1","set2","limit","3").expect.to.equal(":3\r\n");
        ks.run("SINTERCARD","2","set1","set2","limit","10").expect.to.equal(":6\r\n");

        ks.run("SINTERSTORE","setres","set1","set2").expect.to.equal(":6\r\n");
        sameSet(ks.run("SMEMBERS","setres"), "195","196","197","198","199","foo");

        // three sets
        sameSet(ks.run("SINTER","set1","set2","set3"), "195","199","foo");
        ks.run("SINTERCARD","3","set1","set2","set3").expect.to.equal(":3\r\n");
        ks.run("SINTERCARD","3","set1","set2","set3","limit","0").expect.to.equal(":3\r\n");
        ks.run("SINTERCARD","3","set1","set2","set3","limit","2").expect.to.equal(":2\r\n");
        ks.run("SINTERCARD","3","set1","set2","set3","limit","10").expect.to.equal(":3\r\n");

        ks.run("SINTERSTORE","setres","set1","set2","set3").expect.to.equal(":3\r\n");
        sameSet(ks.run("SMEMBERS","setres"), "195","199","foo");

        // SINTER should handle non-existing key as empty (set.tcl:557-562)
        ks.run("SINTER","set1","set2","nokey").expect.to.equal("*0\r\n");
    }

    // SUNION / SUNIONSTORE + non-existing keys (set.tcl:338-369, 630-636, 661-689)
    @("valkey.set.sunion") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","set1","a","b","c");
        ks.run("SADD","set2","b","c","d");

        sameSet(ks.run("SUNION","set1","set2"), "a","b","c","d");
        // non-existing keys treated as empty
        sameSet(ks.run("SUNION","nokey1","set1","set2","nokey2"), "a","b","c","d");
        sameSet(ks.run("SUNION","set1","set2","set3"), "a","b","c","d");

        ks.run("SUNIONSTORE","setres","set1","set2").expect.to.equal(":4\r\n");
        sameSet(ks.run("SMEMBERS","setres"), "a","b","c","d");

        // SUNIONSTORE against non-existing keys deletes the (pre-existing) dstkey
        ks.run("SET","setres","xxx");
        ks.run("SUNIONSTORE","setres","foo111","bar222").expect.to.equal(":0\r\n");
        ks.run("EXISTS","setres").expect.to.equal(":0\r\n");

        // both empty -> delete dstkey
        ks.run("SADD","d1","a","b","c");
        ks.run("SUNIONSTORE","d1","empty1","empty2").expect.to.equal(":0\r\n");
        ks.run("EXISTS","d1").expect.to.equal(":0\r\n");
    }

    // SDIFF / SDIFFSTORE (set.tcl:371-392, 436-543)
    @("valkey.set.sdiff") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","set1","0","1","2","3","4","foo");
        ks.run("SADD","set4","5","6","7","8","foo"); // no small ints
        ks.run("SADD","set5","0");

        // two sets: set1 - set4
        sameSet(ks.run("SDIFF","set1","set4"), "0","1","2","3","4");
        // three sets: set1 - set4 - set5
        sameSet(ks.run("SDIFF","set1","set4","set5"), "1","2","3","4");

        // SDIFFSTORE with three sets
        ks.run("SDIFFSTORE","setres","set1","set4","set5").expect.to.equal(":4\r\n");
        sameSet(ks.run("SMEMBERS","setres"), "1","2","3","4");

        // SDIFF with first set empty -> empty
        ks.run("SDIFF","empty","set4","set5").expect.to.equal("*0\r\n");
        // SDIFF with same set two times -> empty
        ks.run("SDIFF","set1","set1").expect.to.equal("*0\r\n");
        // three same sets: inter/union = self, diff = empty (set.tcl:388-393)
        ks.run("SDIFF","set1","set1","set1").expect.to.equal("*0\r\n");

        // SDIFF should handle non-existing key as empty (set.tcl:490-497)
        ks.run("SADD","a1","a","b","c");
        ks.run("SADD","a2","b","c","d");
        sameSet(ks.run("SDIFF","a1","a2","a3"), "a");
        ks.run("SDIFF","a3","a2","a1").expect.to.equal("*0\r\n");
    }

    // SDIFFSTORE deleting / preserving dstkey (set.tcl:522-543)
    @("valkey.set.sdiffstore_dstkey") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        // non-existing sources: pre-existing string dstkey deleted, returns 0
        ks.run("SET","setres","xxx");
        ks.run("SDIFFSTORE","setres","foo111","bar222").expect.to.equal(":0\r\n");
        ks.run("EXISTS","setres").expect.to.equal(":0\r\n");

        // legal dstkey with empty result -> delete dstkey
        ks.run("SADD","set3","a","b","c");
        ks.run("SDIFFSTORE","set3","set1","set2").expect.to.equal(":0\r\n");
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");

        // now with a real result
        ks.run("SADD","set1","a","b","c");
        ks.run("SDIFFSTORE","set3","set1","set2").expect.to.equal(":3\r\n");
        ks.run("EXISTS","set3").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","set3"), "a","b","c");

        // empty second operand producing empty result deletes dstkey
        ks.run("SADD","set3b","a","b","c");
        ks.run("SDIFFSTORE","set3b","set2","set1").expect.to.equal(":0\r\n");
        ks.run("EXISTS","set3b").expect.to.equal(":0\r\n");
    }

    // *STORE / SDIFF / SINTER / SUNION against non-set -> WRONGTYPE (set.tcl:475-659)
    @("valkey.set.setops_wrongtype") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SET","key1","x");
        ks.run("SADD","set1","a","b","c");

        // SDIFF, both orders
        ks.run("SDIFF","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SDIFF","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SDIFF","key1","set1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SDIFF","set1","key1").startsWith("-WRONGTYPE").should.equal(true);

        // SINTER, both orders
        ks.run("SINTER","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SINTER","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SINTER","key1","set1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SINTER","set1","key1").startsWith("-WRONGTYPE").should.equal(true);

        // SUNION, both orders
        ks.run("SUNION","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SUNION","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SUNION","key1","set1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SUNION","set1","key1").startsWith("-WRONGTYPE").should.equal(true);
    }

    // *STORE against non-set: empty dstkey stays absent; existing dstkey preserved
    // (set.tcl:499-520, 574-595, 638-659)
    @("valkey.set.store_wrongtype_dstkey") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SET","key1","x");

        // empty dstkey -> WRONGTYPE, dst not created (SDIFFSTORE)
        ks.run("SDIFFSTORE","set3","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");
        ks.run("SDIFFSTORE","set3","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");

        // legal existing dstkey preserved when a source is wrong-typed
        ks.run("SADD","set1","a","b","c");
        ks.run("SADD","set2","b","c","d");
        ks.run("SADD","set3","e");
        ks.run("SDIFFSTORE","set3","key1","set1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","set3").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","set3"), "e");
        ks.run("SDIFFSTORE","set3","set1","key1","set2").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","set3").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","set3"), "e");

        // SINTERSTORE: empty dstkey stays absent
        ks.run("SINTERSTORE","si","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","si").expect.to.equal(":0\r\n");
        ks.run("SINTERSTORE","si","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","si").expect.to.equal(":0\r\n");
        // existing dstkey preserved
        ks.run("SADD","si2","e");
        ks.run("SINTERSTORE","si2","key1","set2","noset").startsWith("-WRONGTYPE").should.equal(true);
        sameSet(ks.run("SMEMBERS","si2"), "e");

        // SUNIONSTORE: empty dstkey stays absent
        ks.run("SUNIONSTORE","su","key1","noset").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","su").expect.to.equal(":0\r\n");
        ks.run("SUNIONSTORE","su","noset","key1").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("EXISTS","su").expect.to.equal(":0\r\n");
        // existing dstkey preserved
        ks.run("SADD","su2","e");
        ks.run("SUNIONSTORE","su2","key1","key2","noset").startsWith("-WRONGTYPE").should.equal(true);
        sameSet(ks.run("SMEMBERS","su2"), "e");
    }

    // SINTERSTORE against non-existing keys deletes dstkey (set.tcl:597-615)
    @("valkey.set.sinterstore_missing") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SET","setres","xxx");
        ks.run("SINTERSTORE","setres","foo111","bar222").expect.to.equal(":0\r\n");
        ks.run("EXISTS","setres").expect.to.equal(":0\r\n");

        // legal dstkey, empty intersection -> delete dstkey
        ks.run("SADD","set3","a","b","c");
        ks.run("SINTERSTORE","set3","set1","set2").expect.to.equal(":0\r\n");
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");

        ks.run("SADD","set1","a","b","c");
        ks.run("SINTERSTORE","set3","set1","set2").expect.to.equal(":0\r\n");
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");
        ks.run("SINTERSTORE","set3","set2","set1").expect.to.equal(":0\r\n");
        ks.run("EXISTS","set3").expect.to.equal(":0\r\n");
    }

    // SINTER with same integer elements but different encoding (set.tcl:564-572)
    @("valkey.set.sinter_mixed_encoding") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("SADD","set1","1","2","3");
        ks.run("SADD","set2","1","2","3","a");
        ks.run("SREM","set2","a").expect.to.equal(":1\r\n");
        sameSet(ks.run("SINTER","set1","set2"), "1","2","3");
    }

    // SPOP with and without count (set.tcl:692-792)
    @("valkey.set.spop") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        // SPOP without count returns one member (bulk), draining the set
        ks.run("SADD","s","a","b","c");
        auto p1 = ks.run("SPOP","s"); p1.startsWith("$1\r\n").should.equal(true);
        auto p2 = ks.run("SPOP","s"); p2.startsWith("$1\r\n").should.equal(true);
        auto p3 = ks.run("SPOP","s"); p3.startsWith("$1\r\n").should.equal(true);
        auto got = [parseArrOne(p1), parseArrOne(p2), parseArrOne(p3)];
        sort(got);
        got.expect.to.equal(["a","b","c"]);
        ks.run("SCARD","s").expect.to.equal(":0\r\n");

        // SPOP with count==1 returns a one-element array
        ks.run("SADD","s2","a","b","c");
        auto q1 = ks.run("SPOP","s2","1"); q1.startsWith("*1\r\n").should.equal(true);
        ks.run("SPOP","s2","1");
        ks.run("SPOP","s2","1");
        ks.run("SCARD","s2").expect.to.equal(":0\r\n");

        // SPOP with count >= size returns everything and deletes the key
        ks.run("SADD","s3","a","b","c","d","e");
        sameSet(ks.run("SPOP","s3","30"), "a","b","c","d","e");
        ks.run("EXISTS","s3").expect.to.equal(":0\r\n");

        // SPOP with count of 0 returns an empty array, set intact
        ks.run("SADD","s4","a","b","c");
        ks.run("SPOP","s4","0").expect.to.equal("*0\r\n");
        ks.run("SCARD","s4").expect.to.equal(":3\r\n");

        // SPOP partial count leaves the rest, union of pop + remaining == original
        ks.run("SADD","s5","1","2","3","4","5","6","7","8","9","10");
        auto popped = parseArr(ks.run("SPOP","s5","2"));
        popped.length.expect.to.equal(2);
        ks.run("SCARD","s5").expect.to.equal(":8\r\n");

        // SPOP missing key: nil bulk (no count) / empty array (count)
        ks.run("SPOP","nonexisting_key").expect.to.equal(NIL);
        ks.run("SPOP","nonexisting_key","100").expect.to.equal("*0\r\n");
    }

    // SRANDMEMBER count semantics (set.tcl:817-839, 870-960)
    @("valkey.set.srandmember") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        ks.run("SADD","myset","a","b","c","d","e");

        // count of 0 -> empty array
        ks.run("SRANDMEMBER","myset","0").expect.to.equal("*0\r\n");

        // positive count >= size returns the whole set (unique, unordered)
        sameSet(ks.run("SRANDMEMBER","myset","5"), "a","b","c","d","e");
        sameSet(ks.run("SRANDMEMBER","myset","100"), "a","b","c","d","e");

        // positive count < size returns exactly that many distinct members
        auto three = parseArr(ks.run("SRANDMEMBER","myset","3"));
        three.length.expect.to.equal(3);
        sort(three);
        // all distinct and subset of the set
        (three[0] != three[1]).expect.to.equal(true);
        (three[1] != three[2]).expect.to.equal(true);

        // negative count -> that many WITH repetition allowed (length == abs(count))
        parseArr(ks.run("SRANDMEMBER","myset","-10")).length.expect.to.equal(10);

        // single-member form (no count) -> a bulk of one existing member
        auto one = ks.run("SRANDMEMBER","myset"); one.startsWith("$1\r\n").should.equal(true);

        // against non-existing key: nil (no count) / empty array (with count)
        ks.run("SRANDMEMBER","nonexisting_key").expect.to.equal(NIL);
        ks.run("SRANDMEMBER","nonexisting_key","100").expect.to.equal("*0\r\n");

        // count overflow (LLONG_MIN) -> range error, not a crash
        ks.run("SADD","one","a");
        ks.run("SRANDMEMBER","one","-9223372036854775808").startsWith("-ERR").should.equal(true);
    }

    // SMOVE basics + edge cases (set.tcl:1034-1110)
    @("valkey.set.smove") unittest {
        Keyspace ks; scope(exit) ks.d.free();

        // move a member between sets
        ks.run("SADD","myset1","1","a","b");
        ks.run("SADD","myset2","2","3","4");
        ks.run("SMOVE","myset1","myset2","a").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","myset1"), "1","b");
        sameSet(ks.run("SMEMBERS","myset2"), "2","3","4","a");

        // move an integer member
        ks.run("DEL","myset1"); ks.run("DEL","myset2");
        ks.run("SADD","myset1","1","a","b");
        ks.run("SADD","myset2","2","3","4");
        ks.run("SMOVE","myset1","myset2","1").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","myset1"), "a","b");
        sameSet(ks.run("SMEMBERS","myset2"), "1","2","3","4");

        // move to a non-existing destination creates it
        ks.run("DEL","myset1"); ks.run("DEL","myset3");
        ks.run("SADD","myset1","1","a","b");
        ks.run("SMOVE","myset1","myset3","a").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","myset1"), "1","b");
        sameSet(ks.run("SMEMBERS","myset3"), "a");

        // element not a member of src -> 0, sets unchanged
        ks.run("DEL","myset1"); ks.run("DEL","myset2");
        ks.run("SADD","myset1","1","a","b");
        ks.run("SADD","myset2","2","3","4");
        ks.run("SMOVE","myset1","myset2","foo").expect.to.equal(":0\r\n");
        ks.run("SMOVE","myset1","myset1","foo").expect.to.equal(":0\r\n");
        sameSet(ks.run("SMEMBERS","myset1"), "1","a","b");
        sameSet(ks.run("SMEMBERS","myset2"), "2","3","4");

        // non-existing src set -> 0, dst unchanged
        ks.run("SMOVE","noset","myset2","foo").expect.to.equal(":0\r\n");
        sameSet(ks.run("SMEMBERS","myset2"), "2","3","4");

        // wrong-type src / dst -> WRONGTYPE
        ks.run("SET","xstr","10");
        ks.run("SMOVE","xstr","myset2","foo").startsWith("-WRONGTYPE").should.equal(true);
        ks.run("SMOVE","myset2","xstr","foo").startsWith("-WRONGTYPE").should.equal(true);

        // identical source and destination: moving an existing member is a no-op success
        ks.run("DEL","sset");
        ks.run("SADD","sset","a","b","c");
        ks.run("SMOVE","sset","sset","b").expect.to.equal(":1\r\n");
        sameSet(ks.run("SMEMBERS","sset"), "a","b","c");
    }

    // helper: extract the single bulk string from a "$len\r\nval\r\n" reply
    private string parseArrOne(string s){
        if(s.length==0 || s[0]!='$') return null;
        immutable nl=s.indexOf("\r\n"); immutable ln=s[1..nl].to!int;
        if(ln<0) return null;
        immutable start=nl+2; return s[start..start+ln];
    }
}
