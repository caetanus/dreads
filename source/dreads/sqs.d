// Amazon SQS-compatible skin (production parity, SQS-PLAN.md). A hand-rolled
// HTTP/1.1 server speaking the SQS JSON protocol (X-Amz-Target: AmazonSQS.<Op>,
// application/x-amz-json-1.0) that boto3 and aws-cli use. The structural bet of
// the one-ring vision: an SQS queue IS a keyspace list (sqs.q.<name>) — Send is
// RPUSH, Receive is a pop with a visibility timeout, Delete removes the in-flight
// copy. Every data op routes through gSqsExec (amqpDataExec: hops to the key's
// owner shard), so it is correct under sharding and durable via the per-shard
// AOF for free.
module dreads.sqs;

import vibe.core.net : listenTCP, TCPConnection;
import vibe.core.stream : IOMode;
import core.time : seconds;
import dreads.mem : ByteBuffer;
import dreads.config : gConfig;
import dreads.tls : md5Hex, randHex;

// Data-plane exec (installed by the server = amqpDataExec bound to the SQS db).
public __gshared void delegate(scope const(char)[][] args, ref ByteBuffer reply) nothrow gSqsExec;

private enum string Q_PREFIX = "sqs.q."; // the message list
private enum string IF_PREFIX = "sqs.if."; // in-flight (visibility) hash
private enum string Q_REGISTRY = "sqs.queues"; // set of queue names
private enum string DD_PREFIX = "sqs.dd."; // FIFO dedup hash: dedupId -> expiry+msgid
private enum string DL_PREFIX = "sqs.dl."; // delayed messages: id -> visibleAt+record
private enum long DEDUP_WINDOW_MS = 5 * 60 * 1000; // AWS FIFO 5-minute dedup window
private enum string GRP_PREFIX = "sqs.grp."; // FIFO locked message-groups (set)
private enum char SEP = '\x1f'; // record field separator (never in JSON body text? escaped)

public void startSqs() nothrow
{
    import core.stdc.stdio : printf;

    if (gConfig.sqsPort == 0)
        return;
    try
    {
        listenTCP(gConfig.sqsPort,
            delegate(TCPConnection conn) @trusted nothrow { onConn(conn); },
            gConfig.sqsBind);
        printf("dreads SQS skin on port %u\n", cast(uint) gConfig.sqsPort);
    }
    catch (Exception)
    {
    }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

private void onConn(TCPConnection conn) @trusted nothrow
{
    try
    {
        ubyte[65536] buf = void;
        size_t n;
        if (conn.waitForData(30.seconds))
            n = conn.read(buf[], IOMode.once);
        if (n == 0)
        {
            conn.close();
            return;
        }
        auto req = cast(const(char)[]) buf[0 .. n];
        // read the rest of the body if Content-Length exceeds what we have
        immutable clen = header(req, "content-length").toSize;
        size_t hend = bodyStart(buf[0 .. n]);
        ByteBuffer whole;
        if (hend != 0 && clen != 0 && hend + clen > n)
        {
            whole.append(buf[0 .. n]);
            ubyte[16384] tmp = void;
            while (whole.length < hend + clen)
            {
                if (!conn.waitForData(10.seconds))
                    break;
                immutable r = conn.read(tmp[], IOMode.once);
                if (r <= 0)
                    break;
                whole.append(tmp[0 .. r]);
            }
            req = cast(const(char)[]) whole.data;
            hend = bodyStart(cast(const(ubyte)[]) whole.data);
        }
        auto reqBody = hend != 0 && hend <= req.length ? req[hend .. $] : null;

        // action from X-Amz-Target: "AmazonSQS.SendMessage" (JSON protocol)
        auto tgt = header(req, "x-amz-target");
        size_t dot = 0;
        while (dot < tgt.length && tgt[dot] != '.')
            dot++;
        auto action = dot < tgt.length ? tgt[dot + 1 .. $] : tgt;

        ByteBuffer out_;
        immutable ok = dispatch(action, reqBody, out_);
        if (!ok)
            writeErr(conn, 400, "AWS.SimpleQueueService.NonExistentQueue",
                "The specified queue does not exist.");
        else
            writeJson(conn, cast(const(char)[]) out_.data);
        conn.close();
    }
    catch (Exception)
    {
    }
}

private bool dispatch(scope const(char)[] action, scope const(char)[] b, ref ByteBuffer o) @trusted
{
    if (action == "CreateQueue")
        return opCreateQueue(b, o);
    if (action == "GetQueueUrl")
        return opGetQueueUrl(b, o);
    if (action == "ListQueues")
        return opListQueues(b, o);
    if (action == "DeleteQueue")
        return opDeleteQueue(b, o);
    if (action == "GetQueueAttributes")
        return opGetQueueAttributes(b, o);
    if (action == "SetQueueAttributes")
    {
        o.append("{}"); // tolerated no-op
        return true;
    }
    if (action == "SendMessage")
        return opSendMessage(b, o);
    if (action == "SendMessageBatch")
        return opSendMessageBatch(b, o);
    if (action == "ReceiveMessage")
        return opReceiveMessage(b, o);
    if (action == "DeleteMessage")
        return opDeleteMessage(b, o);
    if (action == "DeleteMessageBatch")
        return opDeleteMessageBatch(b, o);
    if (action == "PurgeQueue")
        return opPurgeQueue(b, o);
    return false;
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

private bool opCreateQueue(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = jsonStrRaw(b, "QueueName");
    if (name.length == 0 || !validName(name))
        return false;
    static ByteBuffer rb;
    const(char)[][3] a = ["sadd", Q_REGISTRY, name];
    exec(a[], rb);
    o.append(`{"QueueUrl":"`);
    appendQueueUrl(o, name);
    o.append(`"}`);
    return true;
}

private bool opGetQueueUrl(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = jsonStrRaw(b, "QueueName");
    if (name.length == 0 || !queueExists(name))
        return false;
    o.append(`{"QueueUrl":"`);
    appendQueueUrl(o, name);
    o.append(`"}`);
    return true;
}

private bool opListQueues(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto prefix = jsonStrRaw(b, "QueueNamePrefix");
    static ByteBuffer rb;
    const(char)[][2] a = ["smembers", Q_REGISTRY];
    exec(a[], rb);
    o.append(`{"QueueUrls":[`);
    bool first = true;
    respEachBulk(cast(const(char)[]) rb.data, (scope const(char)[] name) {
        if (prefix.length && !(name.length >= prefix.length && name[0 .. prefix.length] == prefix))
            return;
        if (!first)
            o.append(",");
        first = false;
        o.append(`"`);
        appendQueueUrl(o, name);
        o.append(`"`);
    });
    o.append("]}");
    return true;
}

private bool opDeleteQueue(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0)
        return false;
    static ByteBuffer rb, key;
    const(char)[][3] a1 = ["srem", Q_REGISTRY, name];
    exec(a1[], rb);
    key.clear();
    key.append(Q_PREFIX);
    key.append(name);
    const(char)[][2] a2 = ["del", cast(const(char)[]) key.data];
    exec(a2[], rb);
    key.clear();
    key.append(IF_PREFIX);
    key.append(name);
    const(char)[][2] a3 = ["del", cast(const(char)[]) key.data];
    exec(a3[], rb);
    foreach (pfx; [DD_PREFIX, GRP_PREFIX, DL_PREFIX])
    {
        key.clear();
        key.append(pfx);
        key.append(name);
        const(char)[][2] ad = ["del", cast(const(char)[]) key.data];
        exec(ad[], rb);
    }
    o.append("{}");
    return true;
}

private bool opGetQueueAttributes(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0 || !queueExists(name))
        return false;
    static ByteBuffer rb, key;
    key.clear();
    key.append(Q_PREFIX);
    key.append(name);
    const(char)[][2] a = ["llen", cast(const(char)[]) key.data];
    exec(a[], rb);
    immutable depth = respInt(cast(const(char)[]) rb.data);
    key.clear();
    key.append(IF_PREFIX);
    key.append(name);
    const(char)[][2] a2 = ["hlen", cast(const(char)[]) key.data];
    exec(a2[], rb);
    immutable inflight = respInt(cast(const(char)[]) rb.data);
    o.append(`{"Attributes":{"ApproximateNumberOfMessages":"`);
    appendLong(o, depth < 0 ? 0 : depth);
    o.append(`","ApproximateNumberOfMessagesNotVisible":"`);
    appendLong(o, inflight < 0 ? 0 : inflight);
    o.append(`","QueueArn":"arn:aws:sqs:dreads:000000000000:`);
    appendJsonStr(o, name);
    o.append(`"}}`);
    return true;
}

private bool opSendMessage(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0 || !queueExists(name))
        return false;
    auto group = jsonStrRaw(b, "MessageGroupId");
    auto dedup = jsonStrRaw(b, "MessageDeduplicationId");
    // copy out of jsonStr's shared TLS ub: dedupSeen()/sendOne() park under
    // sharding and a sibling fiber would refill ub.
    ByteBuffer bodyBuf;
    bodyBuf.append(jsonStr(b, "MessageBody"));
    auto msgBody = cast(const(char)[]) bodyBuf.data;
    if (isFifo(name) && group.length == 0)
        return false; // FIFO requires MessageGroupId
    char[32] mid = void, md5 = void;
    // FIFO dedup: a repeat DeduplicationId within the window is a no-op that
    // returns the original MessageId (exactly-once). Stored in sqs.dd.<name>.
    if (isFifo(name) && dedup.length && dedupSeen(name, dedup, mid, md5, msgBody))
    {
        o.append(`{"MessageId":"`);
        o.append(mid[]);
        o.append(`","MD5OfMessageBody":"`);
        o.append(md5[]);
        o.append(`"}`);
        return true;
    }
    long delay = jsonInt(b, "DelaySeconds", 0);
    if (delay < 0)
        delay = 0;
    if (delay > 900)
        delay = 900; // AWS cap
    if (isFifo(name))
        delay = 0; // FIFO does not support per-message delay (AWS restriction)
    sendOne(name, msgBody, group, delay, mid, md5);
    if (isFifo(name) && dedup.length)
        dedupStore(name, dedup, mid, md5);
    o.append(`{"MessageId":"`);
    o.append(mid[]);
    o.append(`","MD5OfMessageBody":"`);
    o.append(md5[]);
    o.append(`"}`);
    return true;
}

// FIFO dedup: returns true (and fills mid/md5) if this DeduplicationId was
// already used — the message is NOT re-enqueued. Value = msgidmd5.
private bool dedupSeen(scope const(char)[] name, scope const(char)[] dedup,
        ref char[32] mid, ref char[32] md5, scope const(char)[] body_) @trusted nothrow
{
    static ByteBuffer rb, key;
    key.clear();
    key.append(DD_PREFIX);
    key.append(name);
    const(char)[][3] a = ["hget", cast(const(char)[]) key.data, dedup];
    exec(a[], rb);
    auto v = respBulk(cast(const(char)[]) rb.data);
    if (v is null)
        return false;
    // value = expiryMs  msgid(32)  md5(32)
    const(char)[] fexp, fmid, fmd5, funused;
    splitRecord(v, fexp, fmid, fmd5, funused);
    import dreads.stream : nowMs;

    long exp = 0;
    foreach (c; fexp)
        if (c >= '0' && c <= '9')
            exp = exp * 10 + (c - '0');
    if (nowMs() > exp)
        return false; // dedup window passed: treat as a fresh message
    if (fmid.length == 32)
        mid[0 .. 32] = fmid[0 .. 32];
    if (fmd5.length == 32)
        md5[0 .. 32] = fmd5[0 .. 32];
    else
        md5Hex(cast(const(ubyte)[]) body_, md5[]);
    return true;
}

private void dedupStore(scope const(char)[] name, scope const(char)[] dedup,
        ref char[32] mid, ref char[32] md5) @trusted nothrow
{
    static ByteBuffer rb, key, val;
    key.clear();
    key.append(DD_PREFIX);
    key.append(name);
    import dreads.stream : nowMs;

    val.clear();
    appendLong2(val, nowMs() + DEDUP_WINDOW_MS);
    val.appendByte(SEP);
    val.append(mid[]);
    val.appendByte(SEP);
    val.append(md5[]);
    const(char)[][4] a = ["hset", cast(const(char)[]) key.data, dedup, cast(const(char)[]) val.data];
    exec(a[], rb);
}

private bool opSendMessageBatch(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0 || !queueExists(name))
        return false;
    o.append(`{"Successful":[`);
    bool first = true;
    // Entries: [{"Id":"..","MessageBody":".."}, ...]
    jsonEachEntry(b, "Entries", (scope const(char)[] entry) {
        auto id = jsonStrRaw(entry, "Id");
        auto egroup = jsonStrRaw(entry, "MessageGroupId");
        // copy out of jsonStr's shared TLS ub before sendOne() may park
        ByteBuffer ebodyBuf;
        ebodyBuf.append(jsonStr(entry, "MessageBody"));
        auto body_ = cast(const(char)[]) ebodyBuf.data;
        long edelay = jsonInt(entry, "DelaySeconds", 0);
        if (edelay < 0) edelay = 0;
        if (edelay > 900) edelay = 900;
        if (isFifo(name)) edelay = 0;
        char[32] mid = void, md5 = void;
        sendOne(name, body_, egroup, edelay, mid, md5);
        if (!first)
            o.append(",");
        first = false;
        o.append(`{"Id":"`);
        appendJsonStr(o, id);
        o.append(`","MessageId":"`);
        o.append(mid[]);
        o.append(`","MD5OfMessageBody":"`);
        o.append(md5[]);
        o.append(`"}`);
    });
    o.append(`],"Failed":[]}`);
    return true;
}

private bool opReceiveMessage(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    import dreads.stream : nowMs;

    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0 || !queueExists(name))
        return false;
    long maxN = jsonInt(b, "MaxNumberOfMessages", 1);
    if (maxN < 1)
        maxN = 1;
    if (maxN > 10)
        maxN = 10; // AWS cap
    long visTimeout = jsonInt(b, "VisibilityTimeout", 30);
    if (visTimeout < 0)
        visTimeout = 0;
    if (visTimeout > 43200)
        visTimeout = 43200; // AWS max (12h); also prevents *1000 long overflow

    static ByteBuffer rb, qkey, ifkey, val, grpkey;
    qkey.clear();
    qkey.append(Q_PREFIX);
    qkey.append(name);
    ifkey.clear();
    ifkey.append(IF_PREFIX);
    ifkey.append(name);
    grpkey.clear();
    grpkey.append(GRP_PREFIX);
    grpkey.append(name);
    immutable fifo = isFifo(name);

    // FIFO delivery snapshots the queue once (LRANGE) and picks messages in
    // order, skipping any whose group is locked (has an in-flight message) or
    // already taken in THIS batch — so groups run in parallel but each group is
    // strictly ordered. A picked record is removed with LREM (records are
    // unique by msgid, so it matches exactly one). Standard queues just LPOP.
    static ByteBuffer snap;
    // Own the snapshot per-call: the scan loop below holds slices into it ACROSS
    // exec() parks (sismember/lrem/sadd). A sibling opReceiveMessage would refill
    // the shared static `snap`/`recs` during a park -> cross-queue disclosure.
    ByteBuffer snapCopy;
    const(char)[][32] batchGroups; // groups locked within this batch (into snapCopy)
    size_t nBatchGroups = 0;
    if (fifo)
    {
        const(char)[][4] lr = ["lrange", cast(const(char)[]) qkey.data, "0", "-1"];
        exec(lr[], snap);
        snapCopy.append(snap.data); // copy before any further (parking) exec
    }
    size_t scanPos = 0;
    // parse the LRANGE reply lazily via an index cursor over its bulk items
    const(char)[][1024] recs; // per-call: slices point into snapCopy, not a shared static
    size_t nrecs = 0;
    if (fifo)
    {
        nrecs = 0;
        respEachBulk(cast(const(char)[]) snapCopy.data, (scope const(char)[] r) {
            if (nrecs < recs.length)
                recs[nrecs++] = r;
        });
    }

    o.append(`{"Messages":[`);
    bool first = true;
    foreach (_; 0 .. maxN)
    {
        const(char)[] rec;
        if (fifo)
        {
            // TODO(FIFO-atomic): sismember -> lrem -> sadd is a non-atomic
            // check-then-act; concurrent receives can double-deliver / bypass the
            // group lock. Fix with an owner-side atomic op (deferred: an in-process
            // lock held across a fiber park can deadlock the shard). Same deferred
            // bucket: dedupSeen()->dedupStore() (send) is the same TOCTOU family —
            // two concurrent same-DeduplicationId sends can both miss and enqueue.
            // find the next deliverable record from scanPos
            rec = null;
            for (; scanPos < nrecs; scanPos++)
            {
                const(char)[] m2, d2, g2, b2;
                splitRecord(recs[scanPos], m2, d2, g2, b2);
                bool takenThisBatch = false;
                foreach (k; 0 .. nBatchGroups)
                    if (batchGroups[k] == g2)
                    {
                        takenThisBatch = true;
                        break;
                    }
                if (takenThisBatch)
                    continue;
                // group locked by an outstanding in-flight message?
                const(char)[][3] sm = ["sismember", cast(const(char)[]) grpkey.data, g2];
                exec(sm[], rb);
                if (respInt(cast(const(char)[]) rb.data) == 1)
                    continue;
                rec = recs[scanPos];
                scanPos++;
                if (nBatchGroups < batchGroups.length)
                    batchGroups[nBatchGroups++] = g2;
                // remove exactly this record from the queue
                const(char)[][4] lrem = ["lrem", cast(const(char)[]) qkey.data, "1", rec];
                exec(lrem[], rb);
                // lock the group until the message is deleted
                const(char)[][3] sadd = ["sadd", cast(const(char)[]) grpkey.data, g2];
                exec(sadd[], rb);
                break;
            }
            if (rec is null)
                break; // nothing deliverable
        }
        else
        {
            const(char)[][2] pop = ["lpop", cast(const(char)[]) qkey.data];
            exec(pop[], rb);
            rec = respBulk(cast(const(char)[]) rb.data);
            if (rec is null)
                break; // queue empty
        }
        // copy the record out of the shared TLS rb/snap: the hset below reuses
        // rb and (under sharding) parks — a sibling fiber refills it, so the
        // response slices must not point into a shared static.
        ByteBuffer recCopy;
        recCopy.append(rec);
        rec = cast(const(char)[]) recCopy.data;
        const(char)[] mid, md5, group, body_;
        splitRecord(rec, mid, md5, group, body_);
        // move to in-flight with a fresh receipt handle + visibility deadline
        char[48] handle = void;
        randHex(handle[], 24);
        immutable deadline = nowMs() + visTimeout * 1000;
        val.clear();
        appendLong2(val, deadline);
        val.appendByte(SEP);
        val.append(rec);
        const(char)[][4] hset = ["hset", cast(const(char)[]) ifkey.data,
            cast(const(char)[]) handle[], cast(const(char)[]) val.data];
        exec(hset[], rb);
        if (!first)
            o.append(",");
        first = false;
        o.append(`{"MessageId":"`);
        appendJsonStr(o, mid);
        o.append(`","ReceiptHandle":"`);
        o.append(handle[]);
        o.append(`","MD5OfBody":"`);
        appendJsonStr(o, md5);
        o.append(`","Body":"`);
        appendJsonStr(o, body_);
        o.append(`"}`);
    }
    o.append("]}");
    return true;
}

// FIFO: unlock the message's group (srem sqs.grp.<name> <group>) so the group's
// next message can be delivered. Reads the in-flight record by receipt handle.
// group2 is passed as an exec ARG (serialized before the reply is clobbered), so
// this is park-safe.
private void fifoUnlock(scope const(char)[] name, ref ByteBuffer ifkey,
        scope const(char)[] handle) @trusted nothrow
{
    if (!isFifo(name))
        return;
    static ByteBuffer rb, grpkey;
    const(char)[][3] hg = ["hget", cast(const(char)[]) ifkey.data, handle];
    exec(hg[], rb);
    auto v = respBulk(cast(const(char)[]) rb.data);
    if (v is null)
        return;
    // v = deadlineMs  (msgid  md5  group  body)
    size_t sep = 0;
    while (sep < v.length && v[sep] != SEP)
        sep++;
    if (sep >= v.length)
        return;
    const(char)[] mid2, md52, group2, body2;
    splitRecord(v[sep + 1 .. $], mid2, md52, group2, body2);
    grpkey.clear();
    grpkey.append(GRP_PREFIX);
    grpkey.append(name);
    const(char)[][3] sr = ["srem", cast(const(char)[]) grpkey.data, group2];
    exec(sr[], rb);
}

private bool opDeleteMessage(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    auto handle = jsonStrRaw(b, "ReceiptHandle");
    if (name.length == 0 || handle.length == 0)
        return false;
    static ByteBuffer rb, ifkey;
    ifkey.clear();
    ifkey.append(IF_PREFIX);
    ifkey.append(name);
    // FIFO: unlock the message's group so its next message can be delivered.
    // At most one message per group is ever in-flight (the lock blocks the
    // rest), so the group is removed unconditionally here.
    fifoUnlock(name, ifkey, handle);
    const(char)[][3] a = ["hdel", cast(const(char)[]) ifkey.data, handle];
    exec(a[], rb);
    o.append("{}");
    return true;
}

private bool opDeleteMessageBatch(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0)
        return false;
    static ByteBuffer rb, ifkey;
    ifkey.clear();
    ifkey.append(IF_PREFIX);
    ifkey.append(name);
    o.append(`{"Successful":[`);
    bool first = true;
    jsonEachEntry(b, "Entries", (scope const(char)[] entry) {
        auto id = jsonStrRaw(entry, "Id");
        auto handle = jsonStrRaw(entry, "ReceiptHandle");
        if (handle.length)
        {
            // FIFO: unlock the message's group before dropping the in-flight copy
            fifoUnlock(name, ifkey, handle);
            const(char)[][3] a = ["hdel", cast(const(char)[]) ifkey.data, handle];
            exec(a[], rb);
        }
        if (!first)
            o.append(",");
        first = false;
        o.append(`{"Id":"`);
        appendJsonStr(o, id);
        o.append(`"}`);
    });
    o.append(`],"Failed":[]}`);
    return true;
}

private bool opPurgeQueue(scope const(char)[] b, ref ByteBuffer o) @trusted
{
    auto name = queueFromUrl(jsonStrRaw(b, "QueueUrl"));
    if (name.length == 0)
        return false;
    static ByteBuffer rb, key;
    key.clear();
    key.append(Q_PREFIX);
    key.append(name);
    const(char)[][2] a = ["del", cast(const(char)[]) key.data];
    exec(a[], rb);
    key.clear();
    key.append(IF_PREFIX);
    key.append(name);
    const(char)[][2] a2 = ["del", cast(const(char)[]) key.data];
    exec(a2[], rb);
    // also drop FIFO group locks, the dedup window and delayed messages, else a
    // purged group stays locked forever (mirror opDeleteQueue's key list).
    foreach (pfx; [DD_PREFIX, GRP_PREFIX, DL_PREFIX])
    {
        key.clear();
        key.append(pfx);
        key.append(name);
        const(char)[][2] ad = ["del", cast(const(char)[]) key.data];
        exec(ad[], rb);
    }
    o.append("{}");
    return true;
}

// One send: build the record msgid\x1fmd5\x1fgroup\x1fbody. delaySeconds == 0
// RPUSHes to the queue now; > 0 parks it in the delayed hash (sqs.dl.<name>,
// field = a random id, value = visibleAtMs \x1f record) — the visibility sweep
// promotes it to the queue when the delay elapses.
private void sendOne(scope const(char)[] name, scope const(char)[] body_,
        scope const(char)[] group, long delaySeconds, ref char[32] midOut, ref char[32] md5Out) @trusted nothrow
{
    import dreads.stream : nowMs;

    randHex(midOut[], 16);
    md5Hex(cast(const(ubyte)[]) body_, md5Out[]);
    static ByteBuffer rb, key, rec;
    rec.clear();
    rec.append(midOut[]);
    rec.appendByte(SEP);
    rec.append(md5Out[]);
    rec.appendByte(SEP);
    rec.append(group);
    rec.appendByte(SEP);
    rec.append(body_);
    if (delaySeconds > 0)
    {
        key.clear();
        key.append(DL_PREFIX);
        key.append(name);
        char[48] did = void;
        randHex(did[], 24);
        static ByteBuffer dval;
        dval.clear();
        appendLong2(dval, nowMs() + delaySeconds * 1000);
        dval.appendByte(SEP);
        dval.append(rec.data);
        const(char)[][4] a = ["hset", cast(const(char)[]) key.data,
            cast(const(char)[]) did[], cast(const(char)[]) dval.data];
        exec(a[], rb);
        return;
    }
    key.clear();
    key.append(Q_PREFIX);
    key.append(name);
    const(char)[][3] a = ["rpush", cast(const(char)[]) key.data, cast(const(char)[]) rec.data];
    exec(a[], rb);
}

// ---------------------------------------------------------------------------
// Visibility sweep — re-push in-flight messages past their deadline. Called by
// a per-shard timer; only acts on queues whose key belongs to this shard.
// ---------------------------------------------------------------------------

public void sqsVisibilitySweep() nothrow @trusted
{
    import dreads.stream : nowMs;
    import dreads.slots : keyToSlot;
    import dreads.shard : shardOfSlot, tShard, sharded;

    if (gSqsExec is null)
        return;
    immutable now = nowMs();
    static ByteBuffer rb, ifkey, qkey, names, val;
    const(char)[][2] la = ["smembers", Q_REGISTRY];
    exec(la[], names);
    // copy the names out: exec reuses TLS buffers on the per-message calls below
    static ByteBuffer namesCopy;
    namesCopy.clear();
    namesCopy.append(names.data);

    respEachBulk(cast(const(char)[]) namesCopy.data, (scope const(char)[] name) {
        // only the shard that owns the in-flight hash sweeps it
        ifkey.clear();
        ifkey.append(IF_PREFIX);
        ifkey.append(name);
        if (sharded() && shardOfSlot(keyToSlot(cast(const(char)[]) ifkey.data)) != tShard)
            return;
        const(char)[][2] hg = ["hgetall", cast(const(char)[]) ifkey.data];
        exec(hg[], rb);
        // parse field/value pairs; collect expired handles + their records
        static const(char)[][256] expHandles;
        static ByteBuffer expRecs; // packed: [u16 len][bytes]...
        size_t nExp = 0;
        expRecs.clear();
        respEachPair(cast(const(char)[]) rb.data, (scope const(char)[] handle, scope const(char)[] v) {
            if (nExp >= expHandles.length)
                return;
            // v = deadlineMs\x1frecord
            size_t sep = 0;
            while (sep < v.length && v[sep] != SEP)
                sep++;
            if (sep >= v.length)
                return;
            long dl = 0;
            foreach (c; v[0 .. sep])
                if (c >= '0' && c <= '9')
                    dl = dl * 10 + (c - '0');
            if (dl > now)
                return; // still invisible
            expHandles[nExp++] = handle;
            auto rec = v[sep + 1 .. $];
            expRecs.appendByte(cast(char)(rec.length >> 24));
            expRecs.appendByte(cast(char)(rec.length >> 16));
            expRecs.appendByte(cast(char)(rec.length >> 8));
            expRecs.appendByte(cast(char)(rec.length & 0xFF));
            expRecs.append(rec);
        });
        // re-push each expired record to the queue front, then drop the handle
        qkey.clear();
        qkey.append(Q_PREFIX);
        qkey.append(name);
        auto packed = cast(const(char)[]) expRecs.data;
        size_t pi = 0;
        foreach (i; 0 .. nExp)
        {
            if (pi + 4 > packed.length)
                break;
            immutable rl = (cast(size_t) cast(ubyte) packed[pi] << 24)
                | (cast(size_t) cast(ubyte) packed[pi + 1] << 16)
                | (cast(size_t) cast(ubyte) packed[pi + 2] << 8) | cast(ubyte) packed[pi + 3];
            pi += 4;
            if (pi + rl > packed.length)
                break;
            auto rec = packed[pi .. pi + rl];
            pi += rl;
            // re-deliver at the FRONT so ordering is preserved (FIFO), and
            // unlock the message's group so it can be picked again
            const(char)[][3] rp = ["lpush", cast(const(char)[]) qkey.data, rec];
            exec(rp[], val);
            const(char)[][3] hd = ["hdel", cast(const(char)[]) ifkey.data, expHandles[i]];
            exec(hd[], val);
            if (isFifo(name))
            {
                const(char)[] rm, rmd5, rgrp, rbody;
                splitRecord(rec, rm, rmd5, rgrp, rbody);
                static ByteBuffer gk;
                gk.clear();
                gk.append(GRP_PREFIX);
                gk.append(name);
                const(char)[][3] sr = ["srem", cast(const(char)[]) gk.data, rgrp];
                exec(sr[], val);
            }
        }

        // Delay queues: promote delayed messages whose visibility time arrived.
        static ByteBuffer dlkey, dlall;
        dlkey.clear();
        dlkey.append(DL_PREFIX);
        dlkey.append(name);
        const(char)[][2] dhg = ["hgetall", cast(const(char)[]) dlkey.data];
        exec(dhg[], dlall);
        static const(char)[][256] dueIds;
        static ByteBuffer dueRecs; // packed [u16 len][bytes]
        size_t nDue = 0;
        dueRecs.clear();
        respEachPair(cast(const(char)[]) dlall.data, (scope const(char)[] id, scope const(char)[] v) {
            if (nDue >= dueIds.length)
                return;
            size_t sep = 0;
            while (sep < v.length && v[sep] != SEP)
                sep++;
            if (sep >= v.length)
                return;
            long va = 0;
            foreach (c; v[0 .. sep])
                if (c >= '0' && c <= '9')
                    va = va * 10 + (c - '0');
            if (va > now)
                return; // not visible yet
            dueIds[nDue++] = id;
            auto rec = v[sep + 1 .. $];
            dueRecs.appendByte(cast(char)(rec.length >> 24));
            dueRecs.appendByte(cast(char)(rec.length >> 16));
            dueRecs.appendByte(cast(char)(rec.length >> 8));
            dueRecs.appendByte(cast(char)(rec.length & 0xFF));
            dueRecs.append(rec);
        });
        {
            auto packed2 = cast(const(char)[]) dueRecs.data;
            size_t pj = 0;
            foreach (i; 0 .. nDue)
            {
                if (pj + 4 > packed2.length)
                    break;
                immutable rl2 = (cast(size_t) cast(ubyte) packed2[pj] << 24)
                    | (cast(size_t) cast(ubyte) packed2[pj + 1] << 16)
                    | (cast(size_t) cast(ubyte) packed2[pj + 2] << 8) | cast(ubyte) packed2[pj + 3];
                pj += 4;
                if (pj + rl2 > packed2.length)
                    break;
                auto rec = packed2[pj .. pj + rl2];
                pj += rl2;
                const(char)[][3] rp2 = ["rpush", cast(const(char)[]) qkey.data, rec];
                exec(rp2[], val);
                const(char)[][3] hd2 = ["hdel", cast(const(char)[]) dlkey.data, dueIds[i]];
                exec(hd2[], val);
            }
        }

        // Dedup window: reap dedup entries whose 5-minute window elapsed, so the
        // hash can't grow without bound on a busy FIFO queue.
        static ByteBuffer ddkey, ddall;
        ddkey.clear();
        ddkey.append(DD_PREFIX);
        ddkey.append(name);
        const(char)[][2] dda = ["hgetall", cast(const(char)[]) ddkey.data];
        exec(dda[], ddall);
        static const(char)[][256] expDedup;
        size_t nDd = 0;
        respEachPair(cast(const(char)[]) ddall.data, (scope const(char)[] id, scope const(char)[] v) {
            if (nDd >= expDedup.length)
                return;
            long ex = 0;
            foreach (c; v)
            {
                if (c == SEP)
                    break;
                if (c >= '0' && c <= '9')
                    ex = ex * 10 + (c - '0');
            }
            if (now > ex)
                expDedup[nDd++] = id;
        });
        foreach (i; 0 .. nDd)
        {
            const(char)[][3] hd3 = ["hdel", cast(const(char)[]) ddkey.data, expDedup[i]];
            exec(hd3[], val);
        }
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

private void exec(scope const(char)[][] args, ref ByteBuffer reply) @trusted nothrow
{
    if (gSqsExec !is null)
        gSqsExec(args, reply);
    else
        reply.clear();
}

private bool queueExists(scope const(char)[] name) @trusted nothrow
{
    static ByteBuffer rb;
    const(char)[][3] a = ["sismember", Q_REGISTRY, name];
    exec(a[], rb);
    return respInt(cast(const(char)[]) rb.data) == 1;
}

private void appendQueueUrl(ref ByteBuffer o, scope const(char)[] name) @trusted nothrow
{
    o.append("http://");
    o.append(gConfig.sqsBind.length ? gConfig.sqsBind : "127.0.0.1");
    o.append(":");
    appendLong(o, gConfig.sqsPort);
    o.append("/000000000000/");
    appendJsonStr(o, name);
}

// The queue name is the last path segment of the URL.
private const(char)[] queueFromUrl(return scope const(char)[] url) @safe @nogc nothrow
{
    if (url.length == 0)
        return null;
    size_t slash = url.length;
    foreach_reverse (i; 0 .. url.length)
        if (url[i] == '/')
        {
            slash = i;
            break;
        }
    return slash < url.length ? url[slash + 1 .. $] : url;
}

private bool validName(scope const(char)[] n) @safe @nogc nothrow
{
    if (n.length == 0 || n.length > 80)
        return false;
    foreach (c; n)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'))
            return false;
    return true;
}

private bool isFifo(scope const(char)[] name) @safe @nogc nothrow
{
    return name.length > 5 && name[$ - 5 .. $] == ".fifo";
}

// record = msgid  md5  group  body  (group is "" for standard queues)
private void splitRecord(return scope const(char)[] rec, out const(char)[] mid,
        out const(char)[] md5, out const(char)[] group, out const(char)[] body_) @trusted @nogc nothrow
{
    const(char)[][4] f;
    size_t nf, st;
    foreach (i; 0 .. rec.length)
        if (rec[i] == SEP && nf < 3)
        {
            f[nf++] = rec[st .. i];
            st = i + 1;
        }
    f[nf] = rec[st .. $];
    mid = f[0];
    md5 = f[1];
    group = f[2];
    body_ = f[3];
}

// --- tiny JSON field extraction (request bodies are small, flat-ish) --------

// Value of a top-level "key":"string" (returns the RAW, still-escaped slice —
// good enough for names/ids/handles which have no escapes; bodies are passed
// through unescaped on send and re-escaped on receive).
// Raw value slice into `j` (no unescaping, no shared buffer) — safe to hold
// across other extractions. For names/urls/handles/ids (no JSON escapes).
private const(char)[] jsonStrRaw(return scope const(char)[] j, scope const(char)[] key) @safe @nogc nothrow
{
    immutable at = findKey(j, key);
    if (at < 0)
        return null;
    size_t i = at;
    while (i < j.length && j[i] != ':')
        i++;
    i++;
    while (i < j.length && (j[i] == ' ' || j[i] == '	'))
        i++;
    if (i >= j.length || j[i] != '"')
        return null;
    i++;
    immutable start = i;
    while (i < j.length && j[i] != '"')
    {
        if (j[i] == '\\' && i + 1 < j.length)
            i++; // skip escaped char (raw slice keeps the backslash form)
        i++;
    }
    return j[start .. i];
}

private const(char)[] jsonStr(return scope const(char)[] j, scope const(char)[] key) @safe @nogc nothrow
{
    immutable at = findKey(j, key);
    if (at < 0)
        return null;
    size_t i = at;
    // skip ws + colon
    while (i < j.length && j[i] != ':')
        i++;
    i++;
    while (i < j.length && (j[i] == ' ' || j[i] == '\t'))
        i++;
    if (i >= j.length || j[i] != '"')
        return null;
    i++;
    immutable start = i;
    // find the closing quote, honoring backslash escapes (unescape into a TLS buf)
    static char[262144] ub; // AWS SQS max message size (256 KiB)
    size_t o = 0;
    while (i < j.length && j[i] != '"')
    {
        if (j[i] == '\\' && i + 1 < j.length)
        {
            i++;
            char c = j[i];
            char dec;
            switch (c)
            {
            case 'n': dec = '\n'; break;
            case 't': dec = '\t'; break;
            case 'r': dec = '\r'; break;
            case '"': dec = '"'; break;
            case '\\': dec = '\\'; break;
            case '/': dec = '/'; break;
            case 'b': dec = '\b'; break;
            case 'f': dec = '\f'; break;
            case 'u':
                // \uXXXX -> keep ASCII, approximate others as '?'
                if (i + 4 < j.length)
                {
                    int cp = 0;
                    foreach (k; 1 .. 5)
                        cp = cp * 16 + hexVal(j[i + k]);
                    i += 4;
                    dec = cp < 128 ? cast(char) cp : '?';
                }
                else
                    dec = '?';
                break;
            default: dec = c; break;
            }
            if (o < ub.length)
                ub[o++] = dec;
            i++;
        }
        else
        {
            if (o < ub.length)
                ub[o++] = j[i];
            i++;
        }
    }
    return ub[0 .. o];
}

private long jsonInt(scope const(char)[] j, scope const(char)[] key, long dflt) @safe @nogc nothrow
{
    immutable at = findKey(j, key);
    if (at < 0)
        return dflt;
    size_t i = at;
    while (i < j.length && j[i] != ':')
        i++;
    i++;
    while (i < j.length && (j[i] == ' ' || j[i] == '\t' || j[i] == '"'))
        i++;
    bool neg = false;
    if (i < j.length && j[i] == '-')
    {
        neg = true;
        i++;
    }
    if (i >= j.length || j[i] < '0' || j[i] > '9')
        return dflt;
    long v = 0;
    while (i < j.length && j[i] >= '0' && j[i] <= '9')
        v = v * 10 + (j[i++] - '0');
    return neg ? -v : v;
}

// Find `"key"` at a shallow level; returns index just after the closing quote,
// or -1. (Flat request bodies; nested Entries handled by jsonEachEntry.)
private long findKey(scope const(char)[] j, scope const(char)[] key) @safe @nogc nothrow
{
    size_t i = 0;
    while (i + key.length + 2 <= j.length)
    {
        if (j[i] == '"' && j[i + 1 .. i + 1 + key.length] == key
                && j[i + 1 + key.length] == '"')
            return cast(long)(i + 1 + key.length + 1);
        i++;
    }
    return -1;
}

// Iterate the objects of an "Entries":[ {...}, {...} ] array, calling dg with
// each object's raw slice (including braces). Shallow brace-matching.
private void jsonEachEntry(scope const(char)[] j, scope const(char)[] key,
        scope void delegate(scope const(char)[]) nothrow dg) @trusted nothrow
{
    immutable at = findKey(j, key);
    if (at < 0)
        return;
    size_t i = at;
    while (i < j.length && j[i] != '[')
        i++;
    if (i >= j.length)
        return;
    i++;
    while (i < j.length)
    {
        while (i < j.length && (j[i] == ' ' || j[i] == ',' || j[i] == '\n'
                || j[i] == '\t' || j[i] == '\r'))
            i++;
        if (i >= j.length || j[i] == ']')
            break;
        if (j[i] != '{')
            break;
        immutable start = i;
        int depth = 0;
        bool inStr = false;
        while (i < j.length)
        {
            char c = j[i];
            if (inStr)
            {
                if (c == '\\')
                    i++;
                else if (c == '"')
                    inStr = false;
            }
            else if (c == '"')
                inStr = true;
            else if (c == '{')
                depth++;
            else if (c == '}')
            {
                depth--;
                if (depth == 0)
                {
                    i++;
                    break;
                }
            }
            i++;
        }
        dg(j[start .. i]);
    }
}

private int hexVal(char c) @safe @nogc nothrow
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return 0;
}

// --- RESP reply parsing -----------------------------------------------------

private long respInt(scope const(char)[] d) @safe @nogc nothrow
{
    if (d.length < 2 || d[0] != ':')
        return -1;
    long v = 0;
    bool neg = d[1] == '-';
    foreach (c; d[neg ? 2 : 1 .. $])
    {
        if (c == '\r')
            break;
        if (c < '0' || c > '9')
            break;
        v = v * 10 + (c - '0');
    }
    return neg ? -v : v;
}

private const(char)[] respBulk(return scope const(char)[] d) @safe @nogc nothrow
{
    if (d.length < 4 || d[0] != '$')
        return null;
    if (d[1] == '-')
        return null; // nil
    size_t i = 1;
    long n = 0;
    while (i < d.length && d[i] != '\r')
        n = n * 10 + (d[i++] - '0');
    i += 2;
    if (n < 0 || i + n > d.length)
        return null;
    return d[i .. i + cast(size_t) n];
}

private void respEachBulk(scope const(char)[] d, scope void delegate(scope const(char)[]) nothrow dg) @trusted nothrow
{
    if (d.length == 0 || d[0] != '*')
        return;
    size_t i = 1;
    long cnt = 0;
    while (i < d.length && d[i] != '\r')
        cnt = cnt * 10 + (d[i++] - '0');
    i += 2;
    foreach (_; 0 .. cnt)
    {
        if (i >= d.length || d[i] != '$')
            break;
        i++;
        long n = 0;
        bool nil = d[i] == '-';
        while (i < d.length && d[i] != '\r')
        {
            if (d[i] >= '0' && d[i] <= '9')
                n = n * 10 + (d[i] - '0');
            i++;
        }
        i += 2;
        if (nil)
            continue;
        if (i + n > d.length)
            break;
        dg(d[i .. i + cast(size_t) n]);
        i += cast(size_t) n + 2;
    }
}

private void respEachPair(scope const(char)[] d,
        scope void delegate(scope const(char)[], scope const(char)[]) nothrow dg) @trusted nothrow
{
    // HGETALL reply: flat array of field,value,field,value...
    const(char)[] pend;
    bool have = false;
    respEachBulk(d, (scope const(char)[] s) {
        if (!have)
        {
            pend = s;
            have = true;
        }
        else
        {
            dg(pend, s);
            have = false;
        }
    });
}

// --- HTTP helpers -----------------------------------------------------------

private const(char)[] header(return scope const(char)[] req, scope const(char)[] nameLower) @safe @nogc nothrow
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

private size_t bodyStart(scope const(ubyte)[] b) @safe @nogc nothrow
{
    if (b.length < 4)
        return 0;
    foreach (i; 0 .. b.length - 3)
        if (b[i] == '\r' && b[i + 1] == '\n' && b[i + 2] == '\r' && b[i + 3] == '\n')
            return i + 4;
    return 0;
}

private size_t toSize(scope const(char)[] s) @safe @nogc nothrow
{
    size_t v = 0;
    foreach (c; s)
    {
        if (c < '0' || c > '9')
            break;
        v = v * 10 + (c - '0');
    }
    return v;
}

private bool ciEq(scope const(char)[] a, scope const(char)[] b) @safe @nogc nothrow
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

private void writeJson(TCPConnection conn, scope const(char)[] json) @trusted
{
    import core.stdc.stdio : snprintf;

    char[192] hdr = void;
    immutable hn = snprintf(hdr.ptr, hdr.length,
        "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
        json.length);
    if (hn > 0)
    {
        try
        {
            conn.write(cast(const(ubyte)[]) hdr[0 .. hn]);
            conn.write(cast(const(ubyte)[]) json);
        }
        catch (Exception)
        {
        }
    }
}

private void writeErr(TCPConnection conn, int code, scope const(char)[] type, scope const(char)[] msg) @trusted
{
    import core.stdc.stdio : snprintf;

    static ByteBuffer body_;
    body_.clear();
    body_.append(`{"__type":"`);
    body_.append(type);
    body_.append(`","message":"`);
    body_.append(msg);
    body_.append(`"}`);
    char[192] hdr = void;
    immutable hn = snprintf(hdr.ptr, hdr.length,
        "HTTP/1.1 %d Bad Request\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
        code, body_.length);
    if (hn > 0)
    {
        try
        {
            conn.write(cast(const(ubyte)[]) hdr[0 .. hn]);
            conn.write(body_.data);
        }
        catch (Exception)
        {
        }
    }
}

private void appendLong(ref ByteBuffer o, long v) @trusted @nogc nothrow
{
    if (v < 0)
    {
        o.appendByte('-');
        v = -v;
    }
    char[20] t = void;
    size_t n;
    if (v == 0)
        t[n++] = '0';
    else
        while (v)
        {
            t[n++] = cast(char)('0' + v % 10);
            v /= 10;
        }
    char[20] r = void;
    foreach (i; 0 .. n)
        r[i] = t[n - 1 - i];
    o.append(r[0 .. n]);
}

private void appendLong2(ref ByteBuffer o, long v) @trusted @nogc nothrow
{
    appendLong(o, v);
}

private void appendJsonStr(ref ByteBuffer o, scope const(char)[] s) @trusted @nogc nothrow
{
    foreach (c; s)
    {
        if (c == '"' || c == '\\')
        {
            o.appendByte('\\');
            o.appendByte(c);
        }
        else if (c == '\n')
            o.append("\\n");
        else if (c == '\r')
            o.append("\\r");
        else if (c == '\t')
            o.append("\\t");
        else if (cast(ubyte) c < 0x20)
        {
            static immutable hx = "0123456789abcdef";
            o.append("\\u00");
            o.appendByte(hx[(c >> 4) & 0xF]);
            o.appendByte(hx[c & 0xF]);
        }
        else
            o.appendByte(c);
    }
}
