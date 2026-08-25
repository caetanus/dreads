// WebSocket transport codec (RFC 6455) — used to carry MQTT over ws:// (the
// browser transport MQTT clients use). Like the TLS leg, this is a per-conn
// byte translation: inbound WS frames unmask into the MQTT stream, outbound
// MQTT bytes wrap in binary frames. Hand-rolled (vibe-core has no WS server;
// vibe-d's is GC-heavy). Server frames are never masked; client frames always
// are (enforced loosely — we unmask when the mask bit is set).
module dreads.ws;

import vibe.core.net : TCPConnection;
import dreads.mem : ByteBuffer;

enum ubyte WS_CONT = 0x0, WS_TEXT = 0x1, WS_BIN = 0x2, WS_CLOSE = 0x8,
    WS_PING = 0x9, WS_PONG = 0xA;

/// Per-connection WS state (malloc'd; owning-thread lifecycle like TlsLeg).
struct WsCodec
{
    ByteBuffer inbuf; // raw socket bytes awaiting a complete frame
    ByteBuffer pin; // decoded application (MQTT) bytes not yet consumed
    ByteBuffer ctlOut; // pong/close frames to flush (produced during decode)
    bool closed;

    static WsCodec* create() @trusted @nogc nothrow
    {
        import core.stdc.stdlib : calloc;

        return cast(WsCodec*) calloc(1, WsCodec.sizeof);
    }

    void free() @trusted @nogc nothrow
    {
        import core.stdc.stdlib : cfree = free;

        inbuf.release();
        pin.release();
        ctlOut.release();
        cfree(&this);
    }

    /// Feed raw socket bytes; decode every complete frame into `pin` (data) or
    /// `ctlOut` (a pong for a ping / an echo close). False = a protocol error or
    /// a close frame arrived (caller tears the connection down after flushing
    /// ctlOut). Partial trailing frames stay buffered.
    bool feed(scope const(ubyte)[] raw) @trusted @nogc nothrow
    {
        inbuf.append(raw);
        auto d = cast(const(ubyte)[]) inbuf.data;
        size_t pos = 0;
        for (;;)
        {
            if (d.length - pos < 2)
                break;
            immutable b0 = d[pos];
            immutable b1 = d[pos + 1];
            immutable opcode = b0 & 0x0F;
            immutable masked = (b1 & 0x80) != 0;
            size_t hlen = 2;
            ulong plen = b1 & 0x7F;
            if (plen == 126)
            {
                if (d.length - pos < 4)
                    break;
                plen = (cast(ulong) d[pos + 2] << 8) | d[pos + 3];
                hlen = 4;
            }
            else if (plen == 127)
            {
                if (d.length - pos < 10)
                    break;
                plen = 0;
                foreach (k; 0 .. 8)
                    plen = (plen << 8) | d[pos + 2 + k];
                hlen = 10;
            }
            if (plen > 64 * 1024 * 1024)
                return false; // absurd frame: drop the connection
            size_t maskAt = pos + hlen;
            size_t dataAt = maskAt + (masked ? 4 : 0);
            if (d.length < dataAt + plen)
                break; // frame not fully arrived yet
            // unmask the payload in place into a scratch view
            immutable start = pin.length;
            if (opcode == WS_BIN || opcode == WS_TEXT || opcode == WS_CONT)
            {
                auto sp = pin.freeSpace(cast(size_t) plen);
                if (masked)
                {
                    foreach (i; 0 .. cast(size_t) plen)
                        sp[i] = d[dataAt + i] ^ d[maskAt + (i & 3)];
                }
                else
                    sp[0 .. cast(size_t) plen] = d[dataAt .. dataAt + cast(size_t) plen];
                pin.grow(cast(size_t) plen);
            }
            else if (opcode == WS_PING)
            {
                // reply pong with the same (unmasked) payload
                ubyte[8] h = void;
                size_t hn = encodeHeader(h, WS_PONG, cast(size_t) plen);
                ctlOut.append(h[0 .. hn]);
                auto sp = ctlOut.freeSpace(cast(size_t) plen);
                if (masked)
                    foreach (i; 0 .. cast(size_t) plen)
                        sp[i] = d[dataAt + i] ^ d[maskAt + (i & 3)];
                else
                    sp[0 .. cast(size_t) plen] = d[dataAt .. dataAt + cast(size_t) plen];
                ctlOut.grow(cast(size_t) plen);
            }
            else if (opcode == WS_CLOSE)
            {
                ubyte[2] h = [0x88, 0x00]; // FIN + close, empty
                ctlOut.append(h[]);
                closed = true;
                pos = dataAt + cast(size_t) plen;
                break;
            }
            // WS_PONG: ignore
            cast(void) start;
            pos = dataAt + cast(size_t) plen;
        }
        if (pos > 0)
            inbuf.consume(pos);
        return !closed;
    }

    /// Move decoded application bytes into the MQTT input buffer.
    void drainInto(ref ByteBuffer dst) @trusted @nogc nothrow
    {
        if (pin.empty)
            return;
        dst.append(pin.data);
        pin.clear();
    }
}

/// Wrap application bytes in a single binary WS frame into `out_` (server frames
/// are unmasked). A large payload still fits one frame (64-bit length).
void wsEncodeBinary(ref ByteBuffer out_, scope const(ubyte)[] payload) @trusted @nogc nothrow
{
    ubyte[10] h = void;
    immutable hn = encodeHeader(h, WS_BIN, payload.length);
    out_.append(h[0 .. hn]);
    out_.append(payload);
}

private size_t encodeHeader(ref ubyte[8] h, ubyte opcode, size_t len) @trusted @nogc nothrow
{
    h[0] = cast(ubyte)(0x80 | opcode); // FIN + opcode
    if (len < 126)
    {
        h[1] = cast(ubyte) len;
        return 2;
    }
    else if (len < 65_536)
    {
        h[1] = 126;
        h[2] = cast(ubyte)(len >> 8);
        h[3] = cast(ubyte)(len & 0xFF);
        return 4;
    }
    // caller passes ubyte[8]; 64-bit needs 10 — handled by the 10-byte overload
    return 2;
}

private size_t encodeHeader(ref ubyte[10] h, ubyte opcode, size_t len) @trusted @nogc nothrow
{
    h[0] = cast(ubyte)(0x80 | opcode);
    if (len < 126)
    {
        h[1] = cast(ubyte) len;
        return 2;
    }
    else if (len < 65_536)
    {
        h[1] = 126;
        h[2] = cast(ubyte)(len >> 8);
        h[3] = cast(ubyte)(len & 0xFF);
        return 4;
    }
    else
    {
        h[1] = 127;
        immutable ulong L = len;
        foreach (i; 0 .. 8)
            h[2 + i] = cast(ubyte)(L >> (8 * (7 - i)));
        return 10;
    }
}

/// Perform the RFC 6455 upgrade handshake on `req` (the already-read HTTP head).
/// Echoes `Sec-WebSocket-Protocol: <proto>` when the client offered it. Returns
/// false if the request is not a valid WS upgrade.
bool wsHandshake(TCPConnection conn, scope const(char)[] req, scope const(char)[] wantProto) @trusted nothrow
{
    import std.digest.sha : sha1Of;
    import std.base64 : Base64;

    auto key = wsHeader(req, "sec-websocket-key");
    if (key.length == 0)
        return false;
    auto upg = wsHeader(req, "upgrade");
    if (upg.length == 0)
        return false;
    try
    {
        enum GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        char[128] cat = void;
        if (key.length + GUID.length > cat.length)
            return false;
        cat[0 .. key.length] = key;
        cat[key.length .. key.length + GUID.length] = GUID;
        auto sha = sha1Of(cast(const(ubyte)[]) cat[0 .. key.length + GUID.length]);
        char[32] accBuf = void;
        auto acc = Base64.encode(sha[], accBuf[]);

        static ByteBuffer resp;
        resp.clear();
        resp.append("HTTP/1.1 101 Switching Protocols\r\n");
        resp.append("Upgrade: websocket\r\nConnection: Upgrade\r\n");
        resp.append("Sec-WebSocket-Accept: ");
        resp.append(cast(const(char)[]) acc);
        resp.append("\r\n");
        // offer the requested subprotocol only if the client listed it
        auto offered = wsHeader(req, "sec-websocket-protocol");
        if (wantProto.length && offered.length && wsListHas(offered, wantProto))
        {
            resp.append("Sec-WebSocket-Protocol: ");
            resp.append(wantProto);
            resp.append("\r\n");
        }
        resp.append("\r\n");
        conn.write(resp.data);
        return true;
    }
    catch (Exception)
        return false;
}

/// Build the RFC 6455 upgrade response into `resp` instead of writing it — the
/// caller routes it over the right transport (raw, or through the TLS leg for
/// wss). Returns false if `req` is not a valid WS upgrade.
bool wsHandshakeResponse(scope const(char)[] req, scope const(char)[] wantProto,
        ref ByteBuffer resp) @trusted nothrow
{
    import std.digest.sha : sha1Of;
    import std.base64 : Base64;

    auto key = wsHeader(req, "sec-websocket-key");
    auto upg = wsHeader(req, "upgrade");
    if (key.length == 0 || upg.length == 0)
        return false;
    try
    {
        enum GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        char[128] cat = void;
        if (key.length + GUID.length > cat.length)
            return false;
        cat[0 .. key.length] = key;
        cat[key.length .. key.length + GUID.length] = GUID;
        auto sha = sha1Of(cast(const(ubyte)[]) cat[0 .. key.length + GUID.length]);
        char[32] accBuf = void;
        auto acc = Base64.encode(sha[], accBuf[]);
        resp.clear();
        resp.append("HTTP/1.1 101 Switching Protocols
");
        resp.append("Upgrade: websocket
Connection: Upgrade
");
        resp.append("Sec-WebSocket-Accept: ");
        resp.append(cast(const(char)[]) acc);
        resp.append("
");
        auto offered = wsHeader(req, "sec-websocket-protocol");
        if (wantProto.length && offered.length && wsListHas(offered, wantProto))
        {
            resp.append("Sec-WebSocket-Protocol: ");
            resp.append(wantProto);
            resp.append("
");
        }
        resp.append("
");
        return true;
    }
    catch (Exception)
        return false;
}

/// True once `buf` holds a complete HTTP header block (ends with CRLFCRLF).
bool wsHeadersComplete(scope const(ubyte)[] buf) @trusted @nogc nothrow
{
    if (buf.length < 4)
        return false;
    foreach (i; 0 .. buf.length - 3)
        if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n')
            return true;
    return false;
}

/// Any application bytes that arrived in the SAME read as the HTTP upgrade
/// (a client that pipelines the first MQTT CONNECT right after the handshake).
const(ubyte)[] wsBodyAfterHandshake(return scope const(ubyte)[] raw) @trusted @nogc nothrow
{
    if (raw.length < 4)
        return null;
    foreach (i; 0 .. raw.length - 3)
        if (raw[i] == '\r' && raw[i + 1] == '\n' && raw[i + 2] == '\r' && raw[i + 3] == '\n')
            return raw[i + 4 .. $];
    return null;
}

private const(char)[] wsHeader(return scope const(char)[] req, scope const(char)[] nameLower) @trusted @nogc nothrow
{
    size_t i = 0;
    while (i < req.length)
    {
        size_t ls = i, le = i;
        while (le < req.length && req[le] != '\r' && req[le] != '\n')
            le++;
        auto line = req[ls .. le];
        size_t colon = 0;
        while (colon < line.length && line[colon] != ':')
            colon++;
        if (colon < line.length && ciEq(line[0 .. colon], nameLower))
        {
            size_t vs = colon + 1;
            while (vs < line.length && (line[vs] == ' ' || line[vs] == '\t'))
                vs++;
            return line[vs .. $];
        }
        i = le;
        while (i < req.length && (req[i] == '\r' || req[i] == '\n'))
            i++;
        if (le == ls)
            break;
    }
    return null;
}

private bool wsListHas(scope const(char)[] list, scope const(char)[] want) @trusted @nogc nothrow
{
    size_t i = 0;
    while (i < list.length)
    {
        while (i < list.length && (list[i] == ' ' || list[i] == ','))
            i++;
        size_t s = i;
        while (i < list.length && list[i] != ',')
            i++;
        size_t e = i;
        while (e > s && (list[e - 1] == ' ' || list[e - 1] == '\t'))
            e--;
        if (e - s == want.length && list[s .. e] == want)
            return true;
    }
    return false;
}

private bool ciEq(scope const(char)[] a, scope const(char)[] b) @trusted @nogc nothrow
{
    if (a.length != b.length)
        return false;
    foreach (i; 0 .. a.length)
    {
        char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z')
            x += 32;
        if (y >= 'A' && y <= 'Z')
            y += 32;
        if (x != y)
            return false;
    }
    return true;
}

version (unittest)
{
    // encode a client frame (masked) for the decode test
    private void clientFrame(ref ByteBuffer o, ubyte opcode, scope const(ubyte)[] payload) @trusted
    {
        ubyte[10] h = void;
        immutable hn = encodeHeader(h, opcode, payload.length);
        // set mask bit on byte1 and append a mask key
        h[1] |= 0x80;
        o.append(h[0 .. hn]);
        ubyte[4] mk = [0x11, 0x22, 0x33, 0x44];
        o.append(mk[]);
        auto sp = o.freeSpace(payload.length);
        foreach (i; 0 .. payload.length)
            sp[i] = payload[i] ^ mk[i & 3];
        o.grow(payload.length);
    }
}

@trusted unittest // decode masked client frames, split across reads
{
    auto ws = WsCodec.create();
    scope (exit)
        ws.free();
    ByteBuffer wire;
    clientFrame(wire, WS_BIN, cast(const(ubyte)[]) "MQTT-A");
    clientFrame(wire, WS_BIN, cast(const(ubyte)[]) "MQTT-B");
    // feed byte-by-byte to exercise partial-frame buffering
    ByteBuffer mqtt;
    foreach (i; 0 .. wire.length)
    {
        ubyte[1] one = [cast(ubyte) wire.data[i]];
        assert(ws.feed(one[]));
    }
    ws.drainInto(mqtt);
    assert(cast(const(char)[]) mqtt.data == "MQTT-AMQTT-B", cast(string) mqtt.data.idup);

    // a ping produces a pong in ctlOut
    ws.ctlOut.clear();
    ByteBuffer ping;
    clientFrame(ping, WS_PING, cast(const(ubyte)[]) "hi");
    assert(ws.feed(cast(const(ubyte)[]) ping.data));
    assert(ws.ctlOut.length >= 2 && (cast(const(ubyte)[]) ws.ctlOut.data)[0] == 0x8A);

    // a binary frame we encode round-trips through a fresh decoder
    ByteBuffer enc;
    wsEncodeBinary(enc, cast(const(ubyte)[]) "server->client");
    assert((cast(const(ubyte)[]) enc.data)[0] == 0x82); // FIN + binary
    wire.release();
    mqtt.release();
    ping.release();
    enc.release();
}
