module tests.syncer_tests;

// Group-commit bookkeeping. The threaded fdatasync and fiber wakeups need a
// live event loop, so those are verified by running the server; here the
// pure SyncState logic (batching, durability tracking) is proven exhaustively.

version (unittest)
{
    import fluent.asserts;

    import dreads.syncer : SyncState;

    @("syncer.group_commit")
    unittest
    {
        SyncState st;
        // a writer needs index 5 durable; the fsync thread claims it
        st.request(5);
        st.claim().expect.to.equal(5UL);
        st.isDurable(5).expect.to.equal(false); // not yet
        // more writers arrive DURING the in-flight fsync
        st.request(8);
        st.request(12);
        st.claim().expect.to.equal(0UL); // busy: cannot start a second fsync
        // the fsync of 5 finishes
        st.complete();
        st.isDurable(5).expect.to.equal(true);
        st.isDurable(8).expect.to.equal(false);
        st.pending().expect.to.equal(true); // 12 > 5, more to do
        // ONE next fsync covers both 8 and 12 — the group commit
        st.claim().expect.to.equal(12UL);
        st.complete();
        st.isDurable(8).expect.to.equal(true);
        st.isDurable(12).expect.to.equal(true);
        st.pending().expect.to.equal(false);
        st.claim().expect.to.equal(0UL); // idle
    }

    @("syncer.monotonic_and_idempotent")
    unittest
    {
        SyncState st;
        st.request(10);
        st.request(3); // lower request never lowers the target
        st.claim().expect.to.equal(10UL);
        st.complete();
        st.durable.expect.to.equal(10UL);
        // already-durable requests are no-ops
        st.request(7);
        st.claim().expect.to.equal(0UL);
        st.isDurable(7).expect.to.equal(true);
        st.isDurable(11).expect.to.equal(false);
        // fresh higher request resumes
        st.request(11);
        st.claim().expect.to.equal(11UL);
        st.complete();
        st.isDurable(11).expect.to.equal(true);
    }
}
