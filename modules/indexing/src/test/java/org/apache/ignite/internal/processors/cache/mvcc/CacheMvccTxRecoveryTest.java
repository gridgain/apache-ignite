/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.NodeMode.CLIENT;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.NodeMode.SERVER;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.TxEndResult.COMMIT;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest.TxEndResult.ROLLBAK;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/** */
public class CacheMvccTxRecoveryTest extends CacheMvccAbstractTest {
    // t0d0 hair split everything related to committed counter
    /** */
    public enum TxEndResult {
        /** */ COMMIT,
        /** */ ROLLBAK
    }

    /** */
    public enum NodeMode {
        /** */ SERVER,
        /** */ CLIENT
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        throw new RuntimeException("Is not supposed to be used");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitNearFailure1() throws Exception {
        checkRecoveryNearFailure(COMMIT, CLIENT);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitNearFailure2() throws Exception {
        checkRecoveryNearFailure(COMMIT, SERVER);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackNearFailure1() throws Exception {
        checkRecoveryNearFailure(ROLLBAK, CLIENT);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackNearFailure2() throws Exception {
        checkRecoveryNearFailure(ROLLBAK, SERVER);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure1(true, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackPrimaryFailure1() throws Exception {
        checkRecoveryPrimaryFailure1(false, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommitPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure1(true, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackPrimaryFailure2() throws Exception {
        checkRecoveryPrimaryFailure1(false, true);
    }

    /** */
    private void checkRecoveryNearFailure(TxEndResult endRes, NodeMode nearNodeMode) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        boolean commit = endRes == COMMIT;

        startGridsMultiThreaded(baseCnt);

        // tweak client/server near
        client = nearNodeMode == CLIENT;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Integer.class));

        Affinity<Object> aff = nearNode.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(0).localNode(), i) && aff.isBackup(grid(1).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(1).localNode(), i) && aff.isBackup(grid(2).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 2;

        TestRecordingCommunicationSpi nearComm
            = (TestRecordingCommunicationSpi)nearNode.configuration().getCommunicationSpi();

        if (!commit)
            nearComm.blockMessages(GridNearTxPrepareRequest.class, grid(1).name());

        GridTestUtils.runAsync(() -> {
            // run in separate thread to exclude tx from thread-local map
            GridNearTxLocal nearTx
                = ((TransactionProxyImpl)nearNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

            for (Integer k : keys)
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

            List<IgniteInternalTx> txs = IntStream.range(0, baseCnt)
                .mapToObj(i -> txsOnNode(grid(i), nearTx.xidVersion()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            IgniteInternalFuture<?> prepareFut = nearTx.prepareNearTxLocal();

            if (commit)
                prepareFut.get();
            else
                assertConditionEventually(() -> txs.stream().anyMatch(tx -> tx.state() == PREPARED));

            // drop near
            nearNode.close();

            assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == (commit ? COMMITTED : ROLLED_BACK)));

            return null;
        }).get();

        if (commit) {
            assertConditionEventually(() -> {
                int rowsCnt = grid(0).cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("select * from Integer")).getAll().size();
                return rowsCnt == keys.size();
            });
        }
        else {
            int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll().size();

            assertEquals(0, rowsCnt);
        }

        for (Integer k : keys) {
            long cntr = -1;

            for (int i = 0; i < baseCnt; i++) {
                IgniteEx g = grid(i);

                if (g.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(g.localNode(), k)) {
                    long c = updateCounter(g.cachex(DEFAULT_CACHE_NAME).context(), k);

                    if (cntr == -1)
                        cntr = c;

                    assertEquals(cntr, c);
                }
            }
        }
    }

    /** */
    private void checkRecoveryPrimaryFailure1(boolean commit, boolean mvccCrd) throws Exception {
        int gridCnt = 4;
        int baseCnt = gridCnt - 1;

        startGridsMultiThreaded(baseCnt);

        client = true;

        IgniteEx nearNode = startGrid(baseCnt);

        IgniteCache<Object, Object> cache = nearNode.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Integer.class));

        Affinity<Object> aff = nearNode.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(0).localNode(), i) && aff.isBackup(grid(1).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(1).localNode(), i) && aff.isBackup(grid(2).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 2;

        int victim, victimBackup;

        if (mvccCrd) {
            victim = 0;
            victimBackup = 1;
        }
        else {
            victim = 1;
            victimBackup = 2;
        }

        TestRecordingCommunicationSpi victimComm = (TestRecordingCommunicationSpi)grid(victim).configuration().getCommunicationSpi();

        if (commit)
            victimComm.blockMessages(GridNearTxFinishResponse.class, nearNode.name());
        else
            victimComm.blockMessages(GridDhtTxPrepareRequest.class, grid(victimBackup).name());

        GridNearTxLocal nearTx
            = ((TransactionProxyImpl)nearNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)).tx();

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, baseCnt)
            .filter(i -> i != victim)
            .mapToObj(i -> txsOnNode(grid(i), nearTx.xidVersion()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        IgniteInternalFuture<IgniteInternalTx> commitFut = nearTx.commitAsync();

        if (commit)
            assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == COMMITTED));
        else
            assertConditionEventually(() -> txs.stream().anyMatch(tx -> tx.state() == PREPARED));

        // drop victim
        grid(victim).close();

        awaitPartitionMapExchange();

        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == (commit ? COMMITTED : ROLLED_BACK)));

        assert victimComm.hasBlockedMessages();

        if (commit) {
            assertConditionEventually(() -> {
                int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("select * from Integer")).getAll().size();
                return rowsCnt == keys.size();
            });
        }
        else {
            int rowsCnt = G.allGrids().get(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll().size();

            assertEquals(0, rowsCnt);
        }

        assertTrue(commitFut.isDone());

        for (Integer k : keys) {
            long cntr = -1;

            for (int i = 0; i < baseCnt; i++) {
                if (i == victim)
                    continue;

                IgniteEx g = grid(i);

                if (g.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(g.localNode(), k)) {
                    long c = updateCounter(g.cachex(DEFAULT_CACHE_NAME).context(), k);

                    if (cntr == -1)
                        cntr = c;

                    assertEquals(cntr, c);
                }
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryCommit() throws Exception {
        // tx is commited
        // mvcc tracker is closed
        // partition counters are consistent
        startGridsMultiThreaded(2);

        client = true;

        IgniteEx ign = startGrid(2);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class));

        AtomicInteger keyCntr = new AtomicInteger();

        ArrayList<Integer> keys = new ArrayList<>();

        ign.cluster().forServers().nodes()
            .forEach(node -> keys.add(keyForNode(ign.affinity("test"), keyCntr, node)));

        GridTestUtils.runAsync(() -> {
            // run in separate thread to exclude tx from thread-local map
            Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

            for (Integer k : keys)
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

            ((TransactionProxyImpl)tx).tx().prepareNearTxLocal().get();

            return null;
        }).get();

        // drop near
        stopGrid(2, true);

        IgniteEx srvNode = grid(0);

        assertConditionEventually(
            () -> srvNode.cache("test").query(new SqlFieldsQuery("select * from Integer")).getAll().size() == 2
        );

        for (int i = 0; i < 2; i++) {
            for (Integer k : keys) {
                dataStore(grid(i).cachex("test").context(), k)
                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryRollbackInconsistentBackups() throws Exception {
        // tx is rolled back
        // mvcc tracker is closed
        // partition counters are consistent
        int srvCnt = 3;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setIndexedTypes(Integer.class, Integer.class));

        ArrayList<Integer> keys = new ArrayList<>();

        Affinity<Object> aff = ign.affinity("test");

        int victim = 2;
        int missedPrepare = 1;

        for (int i = 0; i < 100; i++) {
            if (aff.isPrimary(grid(victim).localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 1;

        // t0d0 choose node wisely and messages to block
        // t0d0 check problem with visibility inconsistency on different nodes in various cases
        ((TestRecordingCommunicationSpi)grid(victim).configuration().getCommunicationSpi())
            .blockMessages(GridDhtTxPrepareRequest.class, grid(missedPrepare).name());

        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
            .filter(i -> i != victim)
            .mapToObj(i -> grid(i).context().cache().context().tm().activeTransactions().iterator().next())
            .collect(Collectors.toList());

        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();

        // drop near
        ign.close();
        // t0d0 delay between multiple failures makes sense (check possible commit + rollback problem)
        // drop primary
        // t0d0 update started voting on mvcc coordinator
        grid(victim).close();

        for (int i = 0; i < srvCnt; i++) {
            if (i == victim) continue;

            for (Integer k : keys) {
                dataStore(grid(i).cachex("test").context(), k)
                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
            }
        }

        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));

        TimeUnit.SECONDS.sleep(5);

        for (int i = 0; i < srvCnt; i++) {
            if (i == victim) continue;

            for (Integer k : keys) {
                dataStore(grid(i).cachex("test").context(), k)
                    .ifPresent(ds -> System.err.println(k + " -> " + ds.updateCounter()));
            }

            System.err.println(grid(i).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryOrphanedRemoteTx() throws Exception {
        // tx is rolled back
        // mvcc tracker is closed
        // partition counters are consistent
        int srvCnt = 2;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setIndexedTypes(Integer.class, Integer.class));

        ArrayList<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            Affinity<Object> aff = ign.affinity("test");
            List<ClusterNode> nodes = new ArrayList<>(aff.mapKeyToPrimaryAndBackups(i));
            ClusterNode primary = nodes.get(0);
            ClusterNode backup = nodes.get(1);
            if (grid(0).localNode().equals(primary) && grid(1).localNode().equals(backup))
                keys.add(i);
            if (grid(1).localNode().equals(primary) && grid(0).localNode().equals(backup))
                keys.add(i);
            if (keys.size() == 2)
                break;
        }

        assert keys.size() == 2;

        TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)ign.configuration().getCommunicationSpi();

        // t0d0 choose node wisely and messages to block
        comm.blockMessages(GridNearTxPrepareRequest.class, grid(1).name());

        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
            .mapToObj(i -> grid(i).context().cache().context().tm().activeTransactions().iterator().next())
            .collect(Collectors.toList());

        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();

        // drop near
        stopGrid(srvCnt, true);

        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));
    }

    /**
     * @throws Exception if failed.
     */
    public void testCountersInterchangeNearFailed() throws Exception {
        // t0d0
    }

    /**
     * @throws Exception if failed.
     */
    public void testCountersInterchangeServerFailed() throws Exception {
        int srvCnt = 3;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setIndexedTypes(Integer.class, Integer.class));

        ArrayList<Integer> keys = new ArrayList<>();

        int vid = 2;

        IgniteEx victim = grid(vid);

        for (int i = 0; i < 1000; i++) {
            if (ign.affinity("test").isPrimary(victim.localNode(), i)) {
                keys.add(i);
                break;
            }
        }

        assert keys.size() == 1;

        TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)victim.configuration().getCommunicationSpi();

        // prevent prepare on one backup
        comm.blockMessages(GridDhtTxPrepareRequest.class, grid(0).name());

        Transaction nearTx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        for (Integer k : keys)
            cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(k));

        List<IgniteInternalTx> txs = IntStream.range(0, srvCnt)
            .mapToObj(this::grid)
            .filter(g -> g != victim)
            .map(g -> g.context().cache().context().tm().activeTransactions().iterator().next())
            .collect(Collectors.toList());

        ((TransactionProxyImpl)nearTx).tx().prepareNearTxLocal();

        // await tx partially prepared
        TimeUnit.SECONDS.sleep(1);

        // drop primary
        stopGrid(victim.name(), true);

        // drop primary (temporary mean because failed only primary freezes grid)
        ign.close();

        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));

        for (int i = 0; i < srvCnt; i++) {
            if (i == vid) continue;

            for (Integer k : keys)
                System.err.println(k + " -> " + updateCounter(grid(i).cachex("test").context(), k));

            System.err.println(grid(i).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testUpdateCountersGapIsClosed() throws Exception {
        int srvCnt = 3;

        startGridsMultiThreaded(srvCnt);

        client = true;

        IgniteEx ign = startGrid(srvCnt);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setIndexedTypes(Integer.class, Integer.class));

        int vid = 1;

        IgniteEx victim = grid(vid);

        ArrayList<Integer> keys = new ArrayList<>();

        Integer part = null;

        Affinity<Object> aff = ign.affinity("test");

        for (int i = 0; i < 2000; i++) {
            int p = aff.partition(i);
            if (aff.isPrimary(victim.localNode(), i)) {
                if (part == null) part = p;
                if (p == part) keys.add(i);
                if (keys.size() == 2) break;
            }
        }

        assert keys.size() == 2;

        Transaction txA = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        // prevent first transaction prepare on one backup
        ((TestRecordingCommunicationSpi)victim.configuration().getCommunicationSpi())
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                final AtomicInteger limiter = new AtomicInteger();
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtTxPrepareRequest)
                        return limiter.getAndIncrement() < 2;
                    return false;
                }
            });

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(keys.get(0)));

        IgniteFuture<Void> futA = txA.commitAsync();

        // await tx partially prepared
        TimeUnit.SECONDS.sleep(1);

        CompletableFuture.runAsync(() -> {
            try (Transaction txB = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, 42)").setArgs(keys.get(1)));

                txB.commit();
            }
        }).join();

        for (int i = 0; i < srvCnt; i++) {
            for (Integer k : keys)
                System.err.println(k + " -> " + updateCounter(grid(i).cachex("test").context(), k));
        }

        // drop primary
        stopGrid(victim.name(), true);

//        assertConditionEventually(() -> txs.stream().allMatch(tx -> tx.state() == ROLLED_BACK));
        TimeUnit.SECONDS.sleep(3);

        for (int i = 0; i < srvCnt; i++) {
            if (i == vid) continue;

            for (Integer k : keys)
                System.err.println(k + " -> " + updateCounter(grid(i).cachex("test").context(), k));

            System.err.println(grid(i).cache("test").query(new SqlFieldsQuery("select * from Integer").setLocal(true)).getAll());
        }
    }

    /** */
    private static CacheConfiguration<Object, Object> basicCcfg() {
        return new CacheConfiguration<>("test")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setCacheMode(PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class);
    }

    /** */
    private static List<IgniteInternalTx> txsOnNode(IgniteEx node, GridCacheVersion xidVer) {
        List<IgniteInternalTx> txs = node.context().cache().context().tm().activeTransactions().stream()
            .peek(tx -> assertEquals(xidVer, tx.nearXidVersion()))
            .collect(Collectors.toList());

        assertFalse(txs.isEmpty());

        return txs;
    }

    /** */
    private static void assertConditionEventually(GridAbsPredicate p)
        throws IgniteInterruptedCheckedException {
        if (!GridTestUtils.waitForCondition(p, 5_000))
            fail();
    }

    /** */
    private static long updateCounter(GridCacheContext<?, ?> cctx, Object key) {
        return dataStore(cctx, key)
            .map(IgniteCacheOffheapManager.CacheDataStore::updateCounter)
            .get();
    }

    /** */
    private static Optional<IgniteCacheOffheapManager.CacheDataStore> dataStore(
        GridCacheContext<?, ?> cctx, Object key) {
        int p = cctx.affinity().partition(key);
        IgniteCacheOffheapManager offheap = cctx.offheap();
        return StreamSupport.stream(offheap.cacheDataStores().spliterator(), false)
            .filter(ds -> ds.partId() == p)
            .findFirst();
    }
}
