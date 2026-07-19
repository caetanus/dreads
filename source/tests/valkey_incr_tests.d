module tests.valkey_incr_tests;
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

    // INCR / DECR basic behavior against non-existing / created keys
    @("valkey.incr.basic") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        // INCR against non existing key -> 1, and GET returns "1"
        ks.run("INCR","novar").expect.to.equal(":1\r\n");
        ks.run("GET","novar").expect.to.equal(bulk("1"));
        // INCR against key created by incr itself
        ks.run("INCR","novar").expect.to.equal(":2\r\n");
        // DECR against key created by incr
        ks.run("DECR","novar").expect.to.equal(":1\r\n");
        // DECR against non-existent key -> -1, then INCR -> 0
        ks.run("DEL","novar_not_exist");
        ks.run("DECR","novar_not_exist").expect.to.equal(":-1\r\n");
        ks.run("INCR","novar_not_exist").expect.to.equal(":0\r\n");
    }

    // INCR / INCRBY / DECRBY on values set with SET, including 32-bit boundaries
    @("valkey.incr.setvalue") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        // INCR against key originally set with SET
        ks.run("SET","novar","100").expect.to.equal("+OK\r\n");
        ks.run("INCR","novar").expect.to.equal(":101\r\n");
        // INCR over 32bit value
        ks.run("SET","novar","17179869184");
        ks.run("INCR","novar").expect.to.equal(":17179869185\r\n");
        // INCRBY over 32bit value with over 32bit increment
        ks.run("SET","novar","17179869184");
        ks.run("INCRBY","novar","17179869184").expect.to.equal(":34359738368\r\n");
        // DECRBY over 32bit value with over 32bit increment, negative result
        ks.run("SET","novar","17179869184");
        ks.run("DECRBY","novar","17179869185").expect.to.equal(":-1\r\n");
        // DECRBY against non-existent key
        ks.run("DEL","key_not_exist");
        ks.run("DECRBY","key_not_exist","1").expect.to.equal(":-1\r\n");
    }

    // INCR error cases: leading/trailing/surrounding spaces are not integers
    @("valkey.incr.spaces") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("SET","novar","    11");
        ks.run("INCR","novar").startsWith("-ERR").expect.to.equal(true);
        ks.run("SET","novar","11    ");
        ks.run("INCR","novar").startsWith("-ERR").expect.to.equal(true);
        ks.run("SET","novar","    11    ");
        ks.run("INCR","novar").startsWith("-ERR").expect.to.equal(true);
    }

    // DECRBY negation overflow (decrement by LLONG_MIN overflows)
    @("valkey.incr.overflow") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("SET","x","0");
        ks.run("DECRBY","x","-9223372036854775808").startsWith("-ERR").expect.to.equal(true);
    }

    // WRONGTYPE: INCR family against a key holding a list
    @("valkey.incr.wrongtype") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("RPUSH","mylist","1").expect.to.equal(":1\r\n");
        ks.run("INCR","mylist").startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("DECR","mylist").startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("INCRBY","mylist","1").startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("DECRBY","mylist","1").startsWith("-WRONGTYPE").expect.to.equal(true);
        ks.run("INCRBYFLOAT","mylist","1.0").startsWith("-WRONGTYPE").expect.to.equal(true);
    }

    // INCRBYFLOAT basic and boundary formatting
    @("valkey.incr.incrbyfloat") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        // Against non-existing key: 1, GET "1", +0.25 -> 1.25, GET "1.25"
        ks.run("DEL","novar");
        ks.run("INCRBYFLOAT","novar","1").expect.to.equal(bulk("1"));
        ks.run("GET","novar").expect.to.equal(bulk("1"));
        ks.run("INCRBYFLOAT","novar","0.25").expect.to.equal(bulk("1.25"));
        ks.run("GET","novar").expect.to.equal(bulk("1.25"));
        // Against key originally set with SET: 1.5 + 1.5 = 3
        ks.run("SET","novar","1.5");
        ks.run("INCRBYFLOAT","novar","1.5").expect.to.equal(bulk("3"));
        // Over 32bit value + 1.5
        ks.run("SET","novar","17179869184");
        ks.run("INCRBYFLOAT","novar","1.5").expect.to.equal(bulk("17179869185.5"));
        // Over 32bit value + over 32bit increment
        ks.run("SET","novar","17179869184");
        ks.run("INCRBYFLOAT","novar","17179869184").expect.to.equal(bulk("34359738368"));
        // Decrement: 1 + (-1.1) = -0.1
        ks.run("SET","foo","1");
        ks.run("INCRBYFLOAT","foo","-1.1").expect.to.equal(bulk("-0.1"));
    }

    // INCRBYFLOAT error cases: spaces, NaN/Infinity, invalid float
    @("valkey.incr.incrbyfloat_errors") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        // spaces left / right / both -> "ERR *valid*"
        ks.run("SET","novar","    11");
        auto e1=ks.run("INCRBYFLOAT","novar","1.0");
        e1.startsWith("-ERR").expect.to.equal(true);
        (e1.indexOf("valid")>=0).expect.to.equal(true);
        ks.run("SET","novar","11    ");
        (ks.run("INCRBYFLOAT","novar","1.0").indexOf("valid")>=0).expect.to.equal(true);
        ks.run("SET","novar"," 11 ");
        (ks.run("INCRBYFLOAT","novar","1.0").indexOf("valid")>=0).expect.to.equal(true);
        // NaN or Infinity not allowed -> "ERR *would produce*"
        ks.run("SET","foo","0");
        auto e2=ks.run("INCRBYFLOAT","foo","+inf");
        e2.startsWith("-ERR").expect.to.equal(true);
        (e2.indexOf("would produce")>=0).expect.to.equal(true);
        // invalid float increment
        ks.run("DEL","mykeyincr");
        auto e3=ks.run("INCRBYFLOAT","mykeyincr","v");
        e3.startsWith("-ERR").expect.to.equal(true);
        (e3.indexOf("valid")>=0).expect.to.equal(true);
        // string with embedded NUL terminator ("1\0002") -> not a valid float
        ks.run("SET","foo","1");
        ks.run("SETRANGE","foo","2","2").expect.to.equal(":3\r\n");
        auto e4=ks.run("INCRBYFLOAT","foo","1");
        e4.startsWith("-ERR").expect.to.equal(true);
        (e4.indexOf("valid")>=0).expect.to.equal(true);
    }

    // No negative zero: adding then subtracting equal amounts yields "0" not "-0"
    @("valkey.incr.negzero") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("DEL","foo");
        ks.run("INCRBYFLOAT","foo","0.024390243902439");
        ks.run("INCRBYFLOAT","foo","-0.024390243902439");
        ks.run("GET","foo").expect.to.equal(bulk("0"));
    }

    // INCR / INCRBY / DECRBY / INCRBYFLOAT unhappy path
    @("valkey.incr.unhappy") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        ks.run("DEL","mykeyincr");
        // INCR/DECR take exactly one arg -> wrong number of arguments
        auto ai=ks.run("INCR","mykeyincr","v");
        ai.startsWith("-ERR").expect.to.equal(true);
        (ai.indexOf("wrong number of arguments")>=0).expect.to.equal(true);
        auto ad=ks.run("DECR","mykeyincr","v");
        ad.startsWith("-ERR").expect.to.equal(true);
        (ad.indexOf("wrong number of arguments")>=0).expect.to.equal(true);
        // INCRBY/DECRBY non-integer increment -> not an integer or out of range
        foreach(cmd; ["INCRBY","DECRBY"]) {
            auto r1=ks.run(cmd,"mykeyincr","v");
            r1.startsWith("-ERR").expect.to.equal(true);
            (r1.indexOf("not an integer")>=0).expect.to.equal(true);
            auto r2=ks.run(cmd,"mykeyincr","1.5");
            r2.startsWith("-ERR").expect.to.equal(true);
            (r2.indexOf("not an integer")>=0).expect.to.equal(true);
        }
        // INCRBYFLOAT non-float increment -> not a valid float
        auto rf=ks.run("INCRBYFLOAT","mykeyincr","v");
        rf.startsWith("-ERR").expect.to.equal(true);
        (rf.indexOf("valid float")>=0).expect.to.equal(true);
    }

    // Value re-parses as integer after an APPEND made it a raw string.
    // Covers "operation should update encoding from raw to int" (encoding is
    // server-only; the semantic result value is what is checked here).
    @("valkey.incr.raw_to_int") unittest {
        Keyspace ks; scope(exit) ks.d.free();
        // INCR: 1 -> append "2" => "12" -> INCR => 13
        ks.run("SET","foo","1");
        ks.run("GET","foo").expect.to.equal(bulk("1"));
        ks.run("APPEND","foo","2").expect.to.equal(":2\r\n");
        ks.run("GET","foo").expect.to.equal(bulk("12"));
        ks.run("INCR","foo").expect.to.equal(":13\r\n");
        ks.run("GET","foo").expect.to.equal(bulk("13"));

        // DECR: 1 -> append "2" => "12" -> DECR => 11
        ks.run("SET","foo","1");
        ks.run("APPEND","foo","2");
        ks.run("GET","foo").expect.to.equal(bulk("12"));
        ks.run("DECR","foo").expect.to.equal(":11\r\n");

        // INCRBY 1: "12" -> 13
        ks.run("SET","foo","1");
        ks.run("APPEND","foo","2");
        ks.run("INCRBY","foo","1").expect.to.equal(":13\r\n");

        // DECRBY 1: "12" -> 11
        ks.run("SET","foo","1");
        ks.run("APPEND","foo","2");
        ks.run("DECRBY","foo","1").expect.to.equal(":11\r\n");
    }
}
