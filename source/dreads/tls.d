// TLS engine — OpenSSL over memory BIOs (production drop-in M1, DROPIN-PLAN.md).
//
// The sockets stay 100% vibe's; TLS is a per-connection BYTE TRANSLATION run
// on the owning shard thread:
//
//     socket cipher ──BIO rbio──▶ SSL_read ──▶ plaintext (the parser's inb)
//     reply plain ──SSL_write──▶ BIO wbio ──▶ cipher ──▶ socket
//
// No SSL call ever yields (memory BIOs only), so each burst of SSL work is
// atomic under the cooperative scheduler; callers serialize cipher-drain +
// socket-write under the connection's wlock so TLS records hit the wire in
// SSL's output order.
//
// Bindings are hand-declared extern(C) (the uringraw style — no dub dep);
// the ABI used here is stable across OpenSSL 1.1.x and 3.x.
module dreads.tls;

import dreads.mem : ByteBuffer;

// ---------------------------------------------------------------------------
// OpenSSL ABI (subset)
// ---------------------------------------------------------------------------

struct SSL_CTX;
struct SSL;
struct SSL_METHOD;
struct BIO;
struct BIO_METHOD;

private extern (C) @nogc nothrow
{
    const(SSL_METHOD)* TLS_server_method();
    const(SSL_METHOD)* TLS_client_method();
    SSL_CTX* SSL_CTX_new(const(SSL_METHOD)* m);
    void SSL_CTX_free(SSL_CTX* ctx);
    int SSL_CTX_use_certificate_chain_file(SSL_CTX* ctx, const(char)* file);
    int SSL_CTX_use_PrivateKey_file(SSL_CTX* ctx, const(char)* file, int type);
    int SSL_CTX_check_private_key(const(SSL_CTX)* ctx);
    int SSL_CTX_load_verify_locations(SSL_CTX* ctx, const(char)* caFile, const(char)* caPath);
    void SSL_CTX_set_verify(SSL_CTX* ctx, int mode, void* cb);
    ulong SSL_CTX_set_options(SSL_CTX* ctx, ulong opts);
    long SSL_CTX_ctrl(SSL_CTX* ctx, int cmd, long larg, void* parg);

    SSL* SSL_new(SSL_CTX* ctx);
    void SSL_free(SSL* s);
    void SSL_set_accept_state(SSL* s);
    void SSL_set_connect_state(SSL* s);
    void SSL_set_bio(SSL* s, BIO* rbio, BIO* wbio);
    int SSL_read(SSL* s, void* buf, int num);
    int SSL_write(SSL* s, const(void)* buf, int num);
    int SSL_get_error(const(SSL)* s, int ret);
    int SSL_shutdown(SSL* s);

    const(BIO_METHOD)* BIO_s_mem();
    BIO* BIO_new(const(BIO_METHOD)* m);
    int BIO_write(BIO* b, const(void)* data, int dlen);
    int BIO_read(BIO* b, void* data, int dlen);
    size_t BIO_ctrl_pending(BIO* b);

    ulong ERR_get_error();
    void ERR_error_string_n(ulong e, char* buf, size_t len);
    void ERR_clear_error();
}

private enum SSL_FILETYPE_PEM = 1;
private enum SSL_ERROR_WANT_READ = 2;
private enum SSL_ERROR_WANT_WRITE = 3;
private enum SSL_ERROR_ZERO_RETURN = 6;
private enum SSL_VERIFY_NONE = 0;
private enum SSL_VERIFY_PEER = 1;
private enum SSL_VERIFY_FAIL_IF_NO_PEER_CERT = 2;
private enum SSL_CTRL_SET_MIN_PROTO_VERSION = 123;
private enum TLS1_2_VERSION = 0x0303;
private enum ulong SSL_OP_NO_RENEGOTIATION = 0x40000000UL;

// ---------------------------------------------------------------------------
// Listener context (one per TLS port; built at boot on the main thread —
// SSL_CTX is thread-safe for per-connection SSL_new afterwards)
// ---------------------------------------------------------------------------

/// tls-auth-clients: no (never request a client cert) | optional | yes (mTLS).
enum TlsClientAuth : ubyte
{
    no,
    optional,
    yes,
}

/// Builds a server SSL_CTX. Returns null and fills err (static storage) on
/// failure — the caller aborts boot; a half-configured TLS listener that
/// silently serves plaintext would be worse than not starting.
SSL_CTX* tlsServerCtx(scope const(char)[] certFile, scope const(char)[] keyFile,
        scope const(char)[] caFile, TlsClientAuth auth, out const(char)[] err) @trusted @nogc nothrow
{
    static char[256] ebuf;
    char[512] zc = void, zk = void, za = void;
    err = null;
    if (certFile.length == 0 || certFile.length >= zc.length
            || keyFile.length == 0 || keyFile.length >= zk.length
            || caFile.length >= za.length)
    {
        err = "tls-cert-file and tls-key-file are required for a TLS port";
        return null;
    }
    zc[0 .. certFile.length] = certFile;
    zc[certFile.length] = 0;
    zk[0 .. keyFile.length] = keyFile;
    zk[keyFile.length] = 0;

    auto ctx = SSL_CTX_new(TLS_server_method());
    if (ctx is null)
    {
        err = "SSL_CTX_new failed (is libssl available?)";
        return null;
    }
    cast(void) SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_2_VERSION, null);
    cast(void) SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION);
    bool fail(const(char)[] what)
    {
        immutable e = ERR_get_error();
        ebuf[0 .. what.length] = what;
        ebuf[what.length] = ':';
        ebuf[what.length + 1] = ' ';
        ERR_error_string_n(e, ebuf.ptr + what.length + 2, ebuf.length - what.length - 2);
        size_t n = what.length + 2;
        while (n < ebuf.length && ebuf[n] != 0)
            n++;
        err = ebuf[0 .. n];
        SSL_CTX_free(ctx);
        return false;
    }

    if (SSL_CTX_use_certificate_chain_file(ctx, zc.ptr) != 1)
        return fail("tls-cert-file") ? ctx : null;
    if (SSL_CTX_use_PrivateKey_file(ctx, zk.ptr, SSL_FILETYPE_PEM) != 1)
        return fail("tls-key-file") ? ctx : null;
    if (SSL_CTX_check_private_key(ctx) != 1)
        return fail("cert/key mismatch") ? ctx : null;
    if (caFile.length)
    {
        za[0 .. caFile.length] = caFile;
        za[caFile.length] = 0;
        if (SSL_CTX_load_verify_locations(ctx, za.ptr, null) != 1)
            return fail("tls-ca-cert-file") ? ctx : null;
    }
    final switch (auth)
    {
    case TlsClientAuth.no:
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null);
        break;
    case TlsClientAuth.optional:
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, null);
        break;
    case TlsClientAuth.yes:
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, null);
        break;
    }
    return ctx;
}

/// Client context for tests/outbound (no peer verification unless caFile given).
SSL_CTX* tlsClientCtx(scope const(char)[] caFile) @trusted @nogc nothrow
{
    auto ctx = SSL_CTX_new(TLS_client_method());
    if (ctx is null)
        return null;
    cast(void) SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_2_VERSION, null);
    if (caFile.length)
    {
        char[512] za = void;
        if (caFile.length >= za.length)
        {
            SSL_CTX_free(ctx);
            return null;
        }
        za[0 .. caFile.length] = caFile;
        za[caFile.length] = 0;
        if (SSL_CTX_load_verify_locations(ctx, za.ptr, null) != 1)
        {
            SSL_CTX_free(ctx);
            return null;
        }
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, null);
    }
    else
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null);
    return ctx;
}

// One SSL_CTX shared by every TLS listener (one cert set for the server, the
// Redis model). Built at boot by the server before any listener/shard starts.
public __gshared SSL_CTX* gTlsCtx;

// ---------------------------------------------------------------------------
// Per-connection engine
// ---------------------------------------------------------------------------

/// One TLS session over memory BIOs. Created and freed ON THE OWNING SHARD
/// THREAD (the allocator contract). The rbio/wbio are owned by the SSL after
/// SSL_set_bio — SSL_free releases them.
struct TlsConn
{
    SSL* ssl;
    BIO* rbio; // we write socket cipher IN here
    BIO* wbio; // SSL writes cipher OUT here; we drain to the socket
    bool failed;
    private ByteBuffer pendingPlain; // plaintext SSL_write couldn't take yet

    /// malloc'd so the pointer lives in Conn without GC involvement.
    static TlsConn* create(SSL_CTX* ctx, bool server) @trusted @nogc nothrow
    {
        import core.stdc.stdlib : calloc;

        auto t = cast(TlsConn*) calloc(1, TlsConn.sizeof);
        if (t is null)
            return null;
        t.ssl = SSL_new(ctx);
        t.rbio = BIO_new(BIO_s_mem());
        t.wbio = BIO_new(BIO_s_mem());
        if (t.ssl is null || t.rbio is null || t.wbio is null)
        {
            t.free();
            return null;
        }
        SSL_set_bio(t.ssl, t.rbio, t.wbio); // SSL owns both from here
        if (server)
            SSL_set_accept_state(t.ssl);
        else
            SSL_set_connect_state(t.ssl);
        return t;
    }

    void free() @trusted @nogc nothrow
    {
        import core.stdc.stdlib : cfree = free;

        if (ssl !is null)
            SSL_free(ssl); // frees rbio/wbio too
        else
        {
            // set_bio never ran: nothing owns the BIOs (leak-free even here)
        }
        pendingPlain.release();
        cfree(&this);
    }

    /// Push socket bytes (cipher) into the engine. Drives the handshake.
    bool feed(scope const(ubyte)[] cipher) @trusted @nogc nothrow
    {
        size_t off = 0;
        while (off < cipher.length)
        {
            immutable chunk = cipher.length - off > int.max ? int.max
                : cast(int)(cipher.length - off);
            immutable n = BIO_write(rbio, cipher.ptr + off, chunk);
            if (n <= 0)
            {
                failed = true;
                return false;
            }
            off += n;
        }
        // handshake progressed: earlier buffered app writes may now go through
        return flushPending();
    }

    /// Drains ALL currently-decryptable plaintext into dst. Returns false on a
    /// fatal error or a clean peer close_notify (either way: close the conn).
    bool readPlain(ref ByteBuffer dst) @trusted @nogc nothrow
    {
        ubyte[16 * 1024] chunk = void; // one TLS record fits
        for (;;)
        {
            ERR_clear_error();
            immutable n = SSL_read(ssl, chunk.ptr, chunk.length);
            if (n > 0)
            {
                dst.append(chunk[0 .. n]);
                continue;
            }
            immutable e = SSL_get_error(ssl, n);
            if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE)
                return true; // need more cipher / need a cipher drain — fine
            failed = true; // ZERO_RETURN (close_notify) or a real error
            return false;
        }
    }

    /// Encrypts plain into the wbio. Bytes SSL can't take yet (handshake not
    /// finished) are buffered and retried on the next feed()/writePlain().
    bool writePlain(scope const(ubyte)[] plain) @trusted @nogc nothrow
    {
        if (failed)
            return false;
        if (!flushPending())
            return false;
        if (!pendingPlain.empty)
        {
            // still blocked on the handshake: queue behind what's already there
            pendingPlain.append(plain);
            return true;
        }
        size_t off = eat(plain);
        if (off < plain.length && !failed)
            pendingPlain.append(plain[off .. $]);
        return !failed;
    }

    /// Moves cipher output to dst (for the socket write). Under wlock with the
    /// write itself, so records keep SSL's order on the wire.
    void drainCipher(ref ByteBuffer dst) @trusted @nogc nothrow
    {
        for (;;)
        {
            immutable pend = BIO_ctrl_pending(wbio);
            if (pend == 0)
                return;
            auto space = dst.freeSpace(pend > 64 * 1024 ? 64 * 1024 : pend);
            immutable n = BIO_read(wbio, space.ptr,
                    space.length > int.max ? int.max : cast(int) space.length);
            if (n <= 0)
                return;
            dst.grow(n);
        }
    }

    /// Best-effort close_notify (cipher lands in wbio; caller drains+writes).
    void shutdown() @trusted @nogc nothrow
    {
        if (!failed && ssl !is null)
            cast(void) SSL_shutdown(ssl);
    }

    private size_t eat(scope const(ubyte)[] plain) @trusted @nogc nothrow
    {
        size_t off = 0;
        while (off < plain.length)
        {
            ERR_clear_error();
            immutable chunk = plain.length - off > int.max ? int.max
                : cast(int)(plain.length - off);
            immutable n = SSL_write(ssl, plain.ptr + off, chunk);
            if (n > 0)
            {
                off += n;
                continue;
            }
            immutable e = SSL_get_error(ssl, n);
            if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE)
                break; // handshake in flight — caller buffers the rest
            failed = true;
            break;
        }
        return off;
    }

    private bool flushPending() @trusted @nogc nothrow
    {
        if (pendingPlain.empty)
            return !failed;
        immutable did = eat(cast(const(ubyte)[]) pendingPlain.data);
        if (failed)
            return false;
        if (did == pendingPlain.length)
            pendingPlain.clear();
        else if (did > 0)
            pendingPlain.consume(did);
        return true;
    }
}

// ---------------------------------------------------------------------------
// Tests — full in-memory handshake + app data, ephemeral cert
// ---------------------------------------------------------------------------

version (unittest)
{
    private string tlsTestCert(out string keyPath) @trusted
    {
        import std.process : execute;
        import std.file : tempDir, exists;
        import std.path : buildPath;

        auto cert = buildPath(tempDir, "dreads_tls_test_cert.pem");
        auto key = buildPath(tempDir, "dreads_tls_test_key.pem");
        keyPath = key;
        if (!exists(cert) || !exists(key))
        {
            auto r = execute(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", key, "-out", cert, "-days", "2", "-nodes",
                "-subj", "/CN=localhost"]);
            assert(r.status == 0, r.output);
        }
        return cert;
    }
}

@trusted unittest // in-memory handshake + bidirectional app data + close_notify
{
    string keyPath;
    auto certPath = tlsTestCert(keyPath);

    const(char)[] err;
    auto sctx = tlsServerCtx(certPath, keyPath, null, TlsClientAuth.no, err);
    assert(sctx !is null, err);
    auto cctx = tlsClientCtx(null);
    assert(cctx !is null);
    scope (exit)
    {
        SSL_CTX_free(sctx);
        SSL_CTX_free(cctx);
    }

    auto srv = TlsConn.create(sctx, true);
    auto cli = TlsConn.create(cctx, false);
    assert(srv !is null && cli !is null);
    scope (exit)
    {
        srv.free();
        cli.free();
    }

    ByteBuffer wire;
    ByteBuffer srvPlain; // everything the server decrypted
    ByteBuffer cliPlain; // everything the client decrypted
    ByteBuffer big;

    // pump: move cipher between the engines AND drive both state machines —
    // readPlain after every feed, exactly like the serve loop does
    void pump()
    {
        foreach (_; 0 .. 16)
        {
            wire.clear();
            cli.drainCipher(wire);
            if (!wire.empty)
                cast(void) srv.feed(cast(const(ubyte)[]) wire.data);
            cast(void) srv.readPlain(srvPlain);
            wire.clear();
            srv.drainCipher(wire);
            if (!wire.empty)
                cast(void) cli.feed(cast(const(ubyte)[]) wire.data);
            cast(void) cli.readPlain(cliPlain);
        }
    }

    // client speaks first (write buffered until the handshake completes)
    assert(cli.writePlain(cast(const(ubyte)[]) "PING\r\n"));
    pump();
    assert(cast(const(char)[]) srvPlain.data == "PING\r\n");
    srvPlain.clear();

    // server replies
    assert(srv.writePlain(cast(const(ubyte)[]) "+PONG\r\n"));
    pump();
    assert(cast(const(char)[]) cliPlain.data == "+PONG\r\n");
    cliPlain.clear();

    // a big payload crosses record boundaries (> 16KB)
    foreach (i; 0 .. 5000)
        big.append("0123456789");
    assert(cli.writePlain(cast(const(ubyte)[]) big.data));
    pump();
    assert(srvPlain.length == big.length);

    // close_notify: server shuts down, the client's engine reports closed
    srv.shutdown();
    pump();
    assert(cli.failed);
    big.release();
    wire.release();
    srvPlain.release();
    cliPlain.release();
}

// ---------------------------------------------------------------------------
// TlsLeg — the per-connection kit the protocol skins embed (engine + the three
// buffers). The skins keep their own socket/lock discipline: every socket
// write of produced cipher happens inside the skin's wlock'd send helper, so
// records keep SSL's output order; the pump here never writes.
// ---------------------------------------------------------------------------

import vibe.core.net : TCPConnection;

public struct TlsLeg
{
    TlsConn* t;
    ByteBuffer cin; // cipher read scratch
    ByteBuffer cout; // cipher write staging (used inside the skin's lock)
    ByteBuffer pin; // decrypted plaintext not yet consumed by the skin

    /// malloc'd; created/freed on the owning shard thread (allocator rule).
    static TlsLeg* create(bool server) @trusted nothrow
    {
        import core.stdc.stdlib : calloc, cfree = free;

        if (gTlsCtx is null)
            return null;
        auto L = cast(TlsLeg*) calloc(1, TlsLeg.sizeof);
        if (L is null)
            return null;
        L.t = TlsConn.create(gTlsCtx, server);
        if (L.t is null)
        {
            cfree(L);
            return null;
        }
        return L;
    }

    void free() @trusted nothrow
    {
        import core.stdc.stdlib : cfree = free;

        if (t !is null)
            t.free();
        cin.release();
        cout.release();
        pin.release();
        cfree(&this);
    }
}

/// ONE blocking socket read, decrypted into pin. False = peer gone or fatal
/// TLS error (close the connection). Handshake output produced here stays in
/// the engine until the skin's next send (call it with null right after).
public bool legPump(TlsLeg* L, TCPConnection tcp) nothrow @trusted
{
    try
    {
        if (!tcp.waitForData())
            return false;
        auto avail = tcp.leastSize;
        if (avail == 0)
            return false;
        if (avail > 256 * 1024)
            avail = 256 * 1024;
        auto cs = L.cin.freeSpace(cast(size_t) avail);
        if (cs.length < cast(size_t) avail)
            return false; // OOM growing the cipher scratch
        tcp.read(cs[0 .. cast(size_t) avail]);
        return L.t.feed(cast(const(ubyte)[]) cs[0 .. cast(size_t) avail])
            && L.t.readPlain(L.pin);
    }
    catch (Exception)
        return false;
}

/// Copies decrypted bytes out of pin (up to dst.length), consuming them.
public size_t legTake(TlsLeg* L, scope ubyte[] dst) nothrow @trusted
{
    immutable n = L.pin.length < dst.length ? L.pin.length : dst.length;
    if (n == 0)
        return 0;
    dst[0 .. n] = (cast(const(ubyte)[]) L.pin.data)[0 .. n];
    L.pin.consume(n);
    return n;
}

/// Moves ALL of pin into the skin's input buffer (the avail-style read loops).
public void legDrainInto(TlsLeg* L, ref ByteBuffer dst) nothrow @trusted
{
    if (L.pin.empty)
        return;
    dst.append(L.pin.data);
    L.pin.clear();
}

/// Encrypt + drain + socket write. The CALLER holds its connection's write
/// lock (the same hold that orders plaintext writers today). plain may be
/// null/empty: that just flushes buffered handshake/pending cipher.
public bool legSend(TlsLeg* L, TCPConnection tcp, scope const(ubyte)[] plain) nothrow @trusted
{
    cast(void) L.t.writePlain(plain);
    L.cout.clear();
    L.t.drainCipher(L.cout);
    bool ok = true;
    if (!L.cout.empty)
    {
        try
            tcp.write(L.cout.data);
        catch (Exception)
            ok = false;
    }
    L.cout.clear();
    return ok && !L.t.failed;
}
