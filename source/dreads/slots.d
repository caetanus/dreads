module dreads.slots;

// Redis Cluster key hashing. A key maps to one of 16384 hash slots:
//   slot = CRC16(hashkey) & 16383
// where hashkey is the substring inside the first `{...}` (the "hash tag") when
// that substring is non-empty, otherwise the whole key. This is exactly Redis's
// scheme, so cluster-aware clients that cache the slot map route to us
// correctly. CRC16 is CCITT (poly 0x1021, MSB-first) — the same table Redis
// uses; we compute it at compile time so the 256 constants can't be mistyped.

enum SLOTS = 16_384;

private immutable ushort[256] crc16tab = () {
    ushort[256] t;
    foreach (i; 0 .. 256)
    {
        ushort crc = cast(ushort)(i << 8);
        foreach (_; 0 .. 8)
            crc = cast(ushort)((crc & 0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1));
        t[i] = crc;
    }
    return t;
}();

// Slicing-by-4 companion tables: crc16x[k][x] = the CRC of byte x followed by
// k zero bytes. A 16-bit register is fully absorbed by the first two bytes of a
// 4-byte block, so the four lookups are INDEPENDENT — the per-byte serial
// dependency chain (the measured hot loop of commandRouteSlot: ~78% of its
// samples) becomes 4 parallel loads + 3 XORs per 4 bytes.
private immutable ushort[256][4] crc16x = () {
    ushort[256][4] t;
    foreach (x; 0 .. 256)
        t[0][x] = crc16tab[x];
    foreach (k; 1 .. 4)
        foreach (x; 0 .. 256)
            t[k][x] = cast(ushort)((t[k - 1][x] << 8) ^ crc16tab[(t[k - 1][x] >> 8) & 0xFF]);
    return t;
}();

ushort crc16(scope const(ubyte)[] buf) @nogc nothrow pure @safe
{
    ushort crc = 0;
    size_t i = 0;
    for (; i + 4 <= buf.length; i += 4)
        crc = cast(ushort)(crc16x[3][((crc >> 8) ^ buf[i]) & 0xFF]
                ^ crc16x[2][(crc ^ buf[i + 1]) & 0xFF]
                ^ crc16x[1][buf[i + 2]] ^ crc16x[0][buf[i + 3]]);
    for (; i < buf.length; i++)
        crc = cast(ushort)((crc << 8) ^ crc16tab[((crc >> 8) ^ buf[i]) & 0xFF]);
    return crc;
}

/// The bytes actually hashed for `key`: the `{tag}` content if a non-empty tag
/// is present, else the whole key. `{}` (empty) and an unclosed `{` hash the
/// whole key — matching Redis's keyHashSlot().
const(char)[] hashSlice(return scope const(char)[] key) @nogc nothrow pure @safe
{
    size_t open = size_t.max;
    foreach (i, c; key)
        if (c == '{')
        {
            open = i;
            break;
        }
    if (open == size_t.max)
        return key;
    foreach (i; open + 1 .. key.length)
        if (key[i] == '}')
            return i > open + 1 ? key[open + 1 .. i] : key; // {} -> whole key
    return key; // '{' with no '}' -> whole key
}

/// The hash slot for `key`.
ushort keyToSlot(scope const(char)[] key) @nogc nothrow pure @safe
{
    return crc16(cast(const(ubyte)[]) hashSlice(key)) & (SLOTS - 1);
}

// ---------------------------------------------------------------------------
// Tests — slot values are cross-checked against Redis's own `CLUSTER KEYSLOT`
// (e.g. redis-cli CLUSTER KEYSLOT foo -> 12182). CRC16("123456789") is the
// CCITT check value 0x31C3, which pins the table/algorithm.
// ---------------------------------------------------------------------------

version (unittest)
{
    import fluent.asserts;

    @("slots.crc16_check_value")
    unittest
    {
        crc16(cast(const(ubyte)[]) "123456789").expect.to.equal(cast(ushort) 0x31C3);
    }

    @("slots.known_redis_slots")
    unittest
    {
        // Values from Redis CLUSTER KEYSLOT.
        keyToSlot("foo").expect.to.equal(cast(ushort) 12_182);
        keyToSlot("bar").expect.to.equal(cast(ushort) 5061);
        keyToSlot("").expect.to.equal(cast(ushort) 0);
        keyToSlot("123456789").expect.to.equal(cast(ushort)(0x31C3 & 16_383));
    }

    @("slots.hash_tags")
    unittest
    {
        // {tag} co-locates keys: {user1000}.following and {user1000}.followers
        // hash the same slot as user1000.
        auto s = keyToSlot("user1000");
        keyToSlot("{user1000}.following").expect.to.equal(s);
        keyToSlot("{user1000}.followers").expect.to.equal(s);
        keyToSlot("foo{}{bar}").expect.to.equal(keyToSlot("foo{}{bar}")); // {} empty -> whole key
        // unclosed tag -> whole key
        keyToSlot("{unclosed").expect.to.equal(keyToSlot("{unclosed"));
        // only the FIRST {...} counts
        keyToSlot("{a}{b}").expect.to.equal(keyToSlot("a"));
    }
}
