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

package org.apache.ignite.internal.processors.cache.distributed;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.ExchangeActions.CacheActionData;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public abstract class CacheBlockOnReadAbstractTest extends GridCommonAbstractTest {
    /** Default cache entries count. */
    private static final int DFLT_CACHE_ENTRIES_CNT = 2 * 1024;

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** List of baseline nodes started at the beginning of the test. */
    protected final List<IgniteEx> baseline = new ArrayList<>();

    /** List of server nodes started at the beginning of the test. */
    protected final List<IgniteEx> srvs = new CopyOnWriteArrayList<>();

    /** List of client nodes started at the beginning of the test. */
    protected final List<IgniteEx> clients = new CopyOnWriteArrayList<>();

    /** Start node in client mode. */
    private volatile boolean startNodesInClientMode;

    /** Latch that is used to wait until all required messages are blocked. */
    private volatile CountDownLatch cntFinishedReadOperations;

    /**
     * Number of baseline servers to start before test.
     *
     * @see Params#baseline()
     */
    protected int baselineServersCount() {
        return currentTestParams().baseline();
    }

    /**
     * Number of non-baseline servers to start before test.
     *
     * @see Params#servers()
     */
    protected int serversCount() {
        return currentTestParams().servers();
    }

    /**
     * Number of clients to start before test.
     *
     * @see Params#clients()
     */
    protected int clientsCount() {
        return currentTestParams().clients();
    }

    /**
     * Number of milliseconds to warmup reading process. Used to lower fluctuations in run time. Might be 0.
     *
     * @see Params#warmup()
     */
    protected long warmup() {
        return currentTestParams().warmup();
    }

    /**
     * Number of milliseconds to wait on the potentially blocking operation.
     *
     * @see Params#timeout()
     */
    protected long timeout() {
        return currentTestParams().timeout();
    }

    /**
     * Cache atomicity mode.
     *
     * @see Params#atomicityMode()
     */
    protected CacheAtomicityMode atomicityMode() {
        return currentTestParams().atomicityMode();
    }

    /**
     * Whether the test should hang or not.
     *
     * @see Params#shouldHang()
     */
    protected boolean shouldHang() {
        return currentTestParams().shouldHang();
    }

    /**
     * @param startNodesInClientMode Start nodes on client mode.
     */
    public void startNodesInClientMode(boolean startNodesInClientMode) {
        this.startNodesInClientMode = startNodesInClientMode;
    }

    /** List of baseline nodes started at the beginning of the test. */
    public List<? extends IgniteEx> baseline() {
        return baseline;
    }

    /** List of server nodes started at the beginning of the test. */
    public List<? extends IgniteEx> servers() {
        return srvs;
    }

    /** List of client nodes started at the beginning of the test. */
    public List<? extends IgniteEx> clients() {
        return clients;
    }

    /**
     * Annotation to configure test methods in {@link CacheBlockOnReadAbstractTest}. Its values are used throughout
     * test implementation.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Params {
        /**
         * Number of baseline servers to start before test.
         */
        int baseline();

        /**
         * Number of non-baseline servers to start before test.
         */
        int servers() default 0;

        /**
         * Number of clients to start before test.
         */
        int clients() default 0;

        /**
         * Number of milliseconds to warmup reading process. Used to lower fluctuations in run time. Might be 0.
         */
        long warmup() default 1000L;

        /**
         * Number of milliseconds to wait on the potentially blocking operation.
         */
        long timeout() default 2000L;

        /**
         * Cache atomicity mode.
         */
        CacheAtomicityMode atomicityMode();

        /**
         * Whether the test should hang or not.
         */
        boolean shouldHang() default false;
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setClientMode(startNodesInClientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();


        // Checking prerequisites.
        assertTrue("Positive timeout is required for the test.", timeout() > 0);

        assertTrue("No baseline servers were requested.", baselineServersCount() > 0);


        int idx = 0;

        // Start baseline nodes.
        for (int i = 0; i < baselineServersCount(); i++)
            baseline.add(startGrid(idx++));

        // Activate cluster.
        baseline.get(0).cluster().active(true);


        // Start server nodes in activated cluster.
        for (int i = 0; i < serversCount(); i++)
            srvs.add(startGrid(idx++));


        // Start client nodes.
        startNodesInClientMode(true);

        for (int i = 0; i < clientsCount(); i++)
            clients.add(startGrid(idx++));
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        baseline.get(0).cluster().active(false);

        stopAllGrids();

        cleanPersistenceDir();
    }


    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = ATOMIC)
    public void testCreateCacheAtomic() throws Exception {
        testCreateCacheTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = TRANSACTIONAL)
    public void testCreateCacheTransactional() throws Exception {
        doTest(
            CacheBlockOnReadAbstractTest::createCachePredicate,
            () -> baseline.get(0).createCache(UUID.randomUUID().toString())
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = ATOMIC)
    public void testDestroyCacheAtomic() throws Exception {
        testDestroyCacheTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = TRANSACTIONAL)
    public void testDestroyCacheTransactional() throws Exception {
        List<String> cacheNames = new ArrayList<>(Arrays.asList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString())
        );

        for (String cacheName : cacheNames)
            baseline.get(0).createCache(cacheName);

        doTest(
            CacheBlockOnReadAbstractTest::destroyCachePredicate,
            () -> baseline.get(0).destroyCache(cacheNames.remove(0))
        );
    }

    // No event :(
//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = ATOMIC)
//    public void testStartClientAtomic() throws Exception {
//        testStartClientTransactional();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = TRANSACTIONAL)
//    public void testStartClientTransactional() throws Exception {
//        startNodesInClientMode(true);
//
//        doTest(
//            discoEvt -> discoEvt.type() == EventType.EVT_NODE_JOINED,
//            () -> startGrid(UUID.randomUUID().toString())
//        );
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 3, servers = 1, clients = 4, atomicityMode = ATOMIC)
//    public void testStopClientAtomic() throws Exception {
//        testStopClientTransactional();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 3, servers = 1, clients = 4, atomicityMode = TRANSACTIONAL)
//    public void testStopClientTransactional() throws Exception {
//        doTest(
//            discoEvt -> discoEvt.type() == EventType.EVT_NODE_LEFT,
//            () -> stopGrid(clients.remove(clients.size() - 1).name())
//        );
//    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = ATOMIC)
    public void testStartServerAtomic() throws Exception {
        testStartServerTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, atomicityMode = TRANSACTIONAL)
    public void testStartServerTransactional() throws Exception {
        startNodesInClientMode(false);

        doTest(
            discoEvt -> discoEvt.type() == EventType.EVT_NODE_JOINED,
            () -> startGrid(UUID.randomUUID().toString())
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 4, clients = 1, atomicityMode = ATOMIC)
    public void testStopServerAtomic() throws Exception {
        testStopServerTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 4, clients = 1, atomicityMode = TRANSACTIONAL)
    public void testStopServerTransactional() throws Exception {
        doTest(
            discoEvt -> discoEvt.type() == EventType.EVT_NODE_LEFT,
            () -> stopGrid(srvs.remove(srvs.size() - 1).name())
        );
    }

//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 6, servers = 1, clients = 1, timeout = 3000L, atomicityMode = ATOMIC)
//    public void testRestartBaselineAtomic() throws Exception {
//        testRestartBaselineTransactional();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @Params(baseline = 6, servers = 1, clients = 1, timeout = 3000L, atomicityMode = TRANSACTIONAL)
//    public void testRestartBaselineTransactional() throws Exception {
//        AtomicInteger baselineIdx = new AtomicInteger(baselineServersCount() - 3);
//
//        doTest(
//            discoEvt -> discoEvt.type() == EventType.EVT_NODE_JOINED,
//            () -> {
//                IgniteEx node = baseline.get(baselineIdx.getAndIncrement());
//
//                TestRecordingCommunicationSpi.spi(node).stopBlock();
//
//                stopGrid(node.name());
//
//                cntFinishedReadOperations.countDown();
//
//                startGrid(node.name());
//            }
//        );
//    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, timeout = 3000L, atomicityMode = ATOMIC)
    public void testUpdateBaselineTopologyAtomic() throws Exception {
        testUpdateBaselineTopologyTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 3, servers = 1, clients = 1, timeout = 3000L, atomicityMode = TRANSACTIONAL)
    public void testUpdateBaselineTopologyTransactional() throws Exception {
        doTest(
            discoEvt -> {
                if (discoEvt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomEvent discoCustomEvt = (DiscoveryCustomEvent)discoEvt;

                    DiscoveryCustomMessage customMsg = discoCustomEvt.customMessage();

                    return customMsg instanceof ChangeGlobalStateMessage;
                }

                return false;
            },
            () -> {
                startNodesInClientMode(false);

                startGrid(UUID.randomUUID().toString());

                baseline.get(0).cluster().setBaselineTopology(baseline.get(0).context().discovery().topologyVersion());
            }
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 9, servers = 1, clients = 1, timeout = 3000L, atomicityMode = ATOMIC)
    public void testStopBaselineAtomic() throws Exception {
        testStopBaselineTransactional();
    }

    /**
     * @throws Exception If failed.
     */
    @Params(baseline = 9, servers = 1, clients = 1, timeout = 3000L, atomicityMode = TRANSACTIONAL)
    public void testStopBaselineTransactional() throws Exception {
        AtomicInteger baselineIdx = new AtomicInteger(baselineServersCount());
        AtomicInteger cntDownCntr = new AtomicInteger(0);

        doTest(
            discoEvt -> discoEvt.type() == EventType.EVT_NODE_LEFT,
            () -> {
                IgniteEx node = baseline.get(baselineIdx.decrementAndGet());

                TestRecordingCommunicationSpi.spi(node).stopBlock();

                for (int i = 0, cnt = cntDownCntr.incrementAndGet(); i < cnt; i++)
                    cntFinishedReadOperations.countDown(); // This node and previously stopped nodes as well.

                stopGrid(node.name());
            }
        );
    }

    /**
     * Checks that given discovery event is from "Create cache" operation.
     *
     * @param discoEvt Discovery event.
     */
    private static boolean createCachePredicate(DiscoveryEvent discoEvt) {
        if (discoEvt instanceof DiscoveryCustomEvent) {

            DiscoveryCustomEvent discoCustomEvt = (DiscoveryCustomEvent)discoEvt;

            DiscoveryCustomMessage customMsg = discoCustomEvt.customMessage();

            if (customMsg instanceof DynamicCacheChangeBatch) {
                DynamicCacheChangeBatch cacheChangeBatch = (DynamicCacheChangeBatch)customMsg;

                ExchangeActions exchangeActions = U.field(cacheChangeBatch, "exchangeActions");

                Collection<CacheActionData> startRequests = exchangeActions.cacheStartRequests();

                return !startRequests.isEmpty();
            }
        }

        return false;
    }

    /**
     * Checks that given discovery event is from "Destroy cache" operation.
     *
     * @param discoEvt Discovery event.
     */
    private static boolean destroyCachePredicate(DiscoveryEvent discoEvt) {
        if (discoEvt instanceof DiscoveryCustomEvent) {

            DiscoveryCustomEvent discoCustomEvt = (DiscoveryCustomEvent)discoEvt;

            DiscoveryCustomMessage customMsg = discoCustomEvt.customMessage();

            if (customMsg instanceof DynamicCacheChangeBatch) {
                DynamicCacheChangeBatch cacheChangeBatch = (DynamicCacheChangeBatch)customMsg;

                ExchangeActions exchangeActions = U.field(cacheChangeBatch, "exchangeActions");

                Collection<CacheActionData> stopRequests = exchangeActions.cacheStopRequests();

                return !stopRequests.isEmpty();
            }
        }

        return false;
    }

    /**
     * Read operation tat is going to be executed during blocking operation.
     */
    @NotNull protected abstract CacheReadBackgroundOperation getReadOperation();

    /**
     * Checks that {@code block} closure blocks or doesn't block read operation
     * (depending on {@link Params#shouldHang()} value). Does it for client, baseline and regular server node.
     *
     * @param blockMsg Predicate that check whether the message corresponds to the {@code block} or not.
     * @param block Blocking operation.
     * @throws Exception If failed.
     */
    public void doTest(Predicate<DiscoveryEvent> blockMsg, RunnableX block) throws Exception {
        BackgroundOperation backgroundOperation = new BlockMessageOnBaselineBackgroundOperation(block, blockMsg);

        CacheReadBackgroundOperation<?, ?> readOperation = getReadOperation();

        readOperation.initCache(baseline.get(0), true);

        // Warmup.
        if (warmup() > 0) {
            try (AutoCloseable read = readOperation.start()) {
                Thread.sleep(warmup());
            }

            assertEquals(
                readOperation.readOperationsFailed() + " read operations failed during warmup.",
                0,
                readOperation.readOperationsFailed()
            );

            assertTrue(
                "No read operations were finished during warmup.",
                readOperation.readOperationsFinishedUnderBlock() > 0
            );
        }


        doTest0(clients.get(0), readOperation, backgroundOperation);

        doTest0(srvs.get(0), readOperation, backgroundOperation);

        doTest0(baseline.get(0), readOperation, backgroundOperation);


        try (AutoCloseable read = readOperation.start()) {
            Thread.sleep(500L);
        }

        assertEquals(
            readOperation.readOperationsFailed() + " read operations failed during finish stage.",
            0,
            readOperation.readOperationsFailed()
        );

        assertTrue(
            "No read operations were finished during finish stage.",
            readOperation.readOperationsFinishedUnderBlock() > 0
        );
    }

    /**
     * Internal part for {@link CacheBlockOnReadAbstractTest#doTest(Predicate, RunnableX)}.
     *
     * @param ignite Ignite instance. Client / baseline / server node.
     * @param readOperation Read operation.
     * @param backgroundOperation Background operation.
     */
    private void doTest0(
        IgniteEx ignite,
        CacheReadBackgroundOperation<?, ?> readOperation,
        BackgroundOperation backgroundOperation
    ) throws Exception {
        // Reinit internal cache state with given ignite instance.
        readOperation.initCache(ignite, false);

        // Ignore coordinator.
        cntFinishedReadOperations = new CountDownLatch(baselineServersCount() - 1);

        // Read while potentially blocking operation is executing.
        try (AutoCloseable block = backgroundOperation.start()) {
            cntFinishedReadOperations.await();

            try (AutoCloseable read = readOperation.start()) {
                Thread.sleep(timeout());
            }
        }
        finally {
            cntFinishedReadOperations = null;
        }

        System.out.println("|> fin " + readOperation.readOperationsFinishedUnderBlock());
        System.out.println("|> max " + readOperation.maxReadDuration() + "ms");

        // None of read operations should fail.
        assertEquals(
            readOperation.readOperationsFailed() + " read operations failed.",
            0,
            readOperation.readOperationsFailed()
        );

        if (shouldHang()) {
            assertEquals(
                readOperation.readOperationsFinishedUnderBlock() + " successfully finished operations found.",
                0,
                readOperation.readOperationsFinishedUnderBlock()
            );

            // One of read operations lasted about as long as blocking timeout.
            assertAlmostEqual(timeout(), readOperation.maxReadDuration());
        }
        else {
            assertTrue(
                "No read operations were finished during timeout.",
                readOperation.readOperationsFinishedUnderBlock() > 0
            );

            // There were no operations as long as blocking timeout.
            assertNotAlmostEqual(timeout(), readOperation.maxReadDuration());

            // On average every read operation was much faster then blocking timeout.
            double avgDuration = (double)timeout() / readOperation.readOperationsFinishedUnderBlock();

            assertTrue(avgDuration < timeout() * 0.1);
        }
    }

    /**
     * Utility class that allows to start and stop some background operation many times.
     */
    protected abstract static class BackgroundOperation {
        /** */
        private IgniteInternalFuture<?> fut;

        /**
         * Invoked strictly before background thread is started.
         */
        protected void init() {
            // No-op.
        }

        /**
         * Operation itself. Will be executed in separate thread. Thread interruption has to be considered as a valid
         * way to stop operation.
         */
        protected abstract void execute();

        /**
         * @return Allowed time to wait in {@link BackgroundOperation#stop()} method before canceling background thread.
         */
        protected abstract long stopTimeout();

        /**
         * Start separate thread and execute method {@link BackgroundOperation#execute()} in it.
         *
         * @return {@link AutoCloseable} that invokes {@link BackgroundOperation#stop()} on closing.
         */
        AutoCloseable start() {
            if (fut != null)
                throw new UnsupportedOperationException("Only one simultanious operation is allowed");

            init();

            CountDownLatch threadStarted = new CountDownLatch(1);

            fut = GridTestUtils.runAsync(() -> {
                try {
                    threadStarted.countDown();

                    execute();
                }
                catch (Exception e) {
                    throw new IgniteException("Unexpected exception in background operation thread", e);
                }
            });

            try {
                threadStarted.await();
            }
            catch (InterruptedException e) {
                try {
                    fut.cancel();
                }
                catch (IgniteCheckedException e1) {
                    e.addSuppressed(e1);
                }

                throw new IgniteException(e);
            }

            return this::stop;
        }

        /**
         * Interrupt the operation started in {@link BackgroundOperation#start()} method and join interrupted thread.
         */
        void stop() throws Exception {
            if (fut == null)
                return;

            try {
                fut.get(stopTimeout());
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                fut.cancel();

                fut.get();
            }
            finally {
                fut = null;
            }
        }
    }

    /**
     * Background operation that executes some node request and doesn't allow its messages to be fully processed until
     * operation is stopped.
     */
    protected class BlockMessageOnBaselineBackgroundOperation extends BackgroundOperation {

        /** */
        private final RunnableX block;

        /** */
        private final Predicate<DiscoveryEvent> blockMsg;

        /**
         * @param block
         * @param blockMsg
         */
        protected BlockMessageOnBaselineBackgroundOperation(
            RunnableX block,
            Predicate<DiscoveryEvent> blockMsg
        ) {
            this.block = block;
            this.blockMsg = blockMsg;
        }

        /** {@inheritDoc} */
        @Override protected void execute() {
            for (IgniteEx server : baseline) {
                TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(server);

                spi.blockMessages(this::blockMessage);
            }

            block.run();
        }

        /**
         * Function to pass into {@link TestRecordingCommunicationSpi#blockMessages(IgniteBiPredicate)}.
         *
         * @param node Node that receives message.
         * @param msg Message.
         * @return Whether the given message should be blocked or not.
         */
        private boolean blockMessage(ClusterNode node, Message msg) {
            boolean block = false;

            if (msg instanceof GridDhtPartitionsAbstractMessage) {
                GridDhtPartitionsAbstractMessage fullMsg = (GridDhtPartitionsAbstractMessage)msg;

                GridDhtPartitionExchangeId exchangeId = fullMsg.exchangeId();

                if (exchangeId != null)
                    block = blockMsg.test(U.field(exchangeId, "discoEvt"));
            }

            if (block)
                cntFinishedReadOperations.countDown();

            return block;
        }

        /** {@inheritDoc} */
        @Override protected long stopTimeout() {
            // Should be big enough so thread will stop by it's own. Otherwise test will fail, but that's fine.
            return 30_000L;
        }

        /** {@inheritDoc} */
        @Override void stop() throws Exception {
            for (IgniteEx server : baseline) {
                TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(server);

                spi.stopBlock();
            }

            super.stop();
        }
    }


    /**
     * Runnable that can throw exceptions.
     */
    @FunctionalInterface
    public interface RunnableX extends Runnable {
        /**
         * Closure body.
         *
         * @throws Exception If failed.
         */
        void runx() throws Exception;

        /** {@inheritdoc} */
        @Override default void run() {
            try {
                runx();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * {@link BackgroundOperation} implementation for cache reading operations.
     */
    protected abstract class ReadBackgroundOperation extends BackgroundOperation {

        /** Counter for successfully finished operations. */
        private final AtomicInteger readOperationsFinishedUnderBlock = new AtomicInteger();

        /** Counter for failed operations. */
        private final AtomicInteger readOperationsFailed = new AtomicInteger();

        /** Duration of the longest read operation. */
        private final AtomicLong maxReadDuration = new AtomicLong(-1);

        /**
         * Do single iteration of reading operation. Will be executed in a loop.
         */
        protected abstract void doRead() throws Exception;


        /** {@inheritDoc} */
        @Override protected void init() {
            readOperationsFinishedUnderBlock.set(0);

            readOperationsFailed.set(0);

            maxReadDuration.set(-1);
        }

        /** {@inheritDoc} */
        @Override protected void execute() {
            while (!Thread.currentThread().isInterrupted()) {
                long prevTs = System.currentTimeMillis();

                try {
                    doRead();

                    readOperationsFinishedUnderBlock.incrementAndGet();
                }
                catch (Exception e) {
                    boolean threadInterrupted = X.hasCause(e,
                        InterruptedException.class,
                        IgniteInterruptedException.class,
                        IgniteInterruptedCheckedException.class
                    );

                    if (threadInterrupted)
                        Thread.currentThread().interrupt();
                    else if (!X.hasCause(e, ClusterTopologyCheckedException.class)) {
                        readOperationsFailed.incrementAndGet();

                        log.error("Error during read operation execution", e);

                        continue;
                    }
                }

                maxReadDuration.set(Math.max(maxReadDuration.get(), System.currentTimeMillis() - prevTs));
            }
        }

        /** {@inheritDoc} */
        @Override protected long stopTimeout() {
            return 0;
        }

        /**
         * @return Number of successfully finished operations.
         */
        public int readOperationsFinishedUnderBlock() {
            return readOperationsFinishedUnderBlock.get();
        }

        /**
         * @return Number of failed operations.
         */
        public int readOperationsFailed() {
            return readOperationsFailed.get();
        }

        /**
         * @return Duration of the longest read operation.
         */
        public long maxReadDuration() {
            return maxReadDuration.get();
        }
    }

    /**
     *
     */
    protected abstract class CacheReadBackgroundOperation<KeyType, ValueType> extends ReadBackgroundOperation {
        /**
         * {@link CacheReadBackgroundOperation#cache()} method backing field. Updated on each
         * {@link CacheReadBackgroundOperation#initCache(IgniteEx, boolean)} invocation.
         */
        private IgniteCache<KeyType, ValueType> cache;

        /**
         * Reinit internal cache using passed ignite node and fill it with data if required.
         *
         * @param ignite Node to get or create cache from.
         * @param fillData Whether the cache should be filled with new data or not.
         */
        public void initCache(IgniteEx ignite, boolean fillData) {
            cache = ignite.getOrCreateCache(createCacheConfiguration().setAtomicityMode(atomicityMode()));

            if (fillData) {
                try (IgniteDataStreamer<KeyType, ValueType> dataStreamer = ignite.dataStreamer(cache.getName())) {
                    dataStreamer.allowOverwrite(true);

                    for (int i = 0; i < entriesCount(); i++)
                        dataStreamer.addData(createKey(i), createValue(i));
                }
            }
        }

        /**
         * @return Cache configuration.
         */
        protected CacheConfiguration<KeyType, ValueType> createCacheConfiguration() {
            return new CacheConfiguration<KeyType, ValueType>(DEFAULT_CACHE_NAME)
                .setBackups(1);
        }

        /**
         * @return Current cache.
         */
        protected final IgniteCache<KeyType, ValueType> cache() {
            return cache;
        }

        /**
         * @return Count of cache entries to create in {@link CacheReadBackgroundOperation#initCache(IgniteEx, boolean)}
         * method.
         */
        protected int entriesCount() {
            return DFLT_CACHE_ENTRIES_CNT;
        }

        /**
         * @param idx Unique number.
         * @return Key to be used for inserting into cache.
         * @see CacheReadBackgroundOperation#createValue(int)
         */
        protected abstract KeyType createKey(int idx);

        /**
         * @param idx Unique number.
         * @return Value to be used for inserting into cache.
         * @see CacheReadBackgroundOperation#createKey(int)
         */
        protected abstract ValueType createValue(int idx);
    }

    /**
     * {@link CacheReadBackgroundOperation} implementation for (int -> int) cache. Keys and values are equal by default.
     */
    protected abstract class IntCacheReadBackgroundOperation
        extends CacheReadBackgroundOperation<Integer, Integer> {
        /** {@inheritDoc} */
        @Override protected Integer createKey(int idx) {
            return idx;
        }

        /** {@inheritDoc} */
        @Override protected Integer createValue(int idx) {
            return idx;
        }
    }

    /**
     * @return {@link Params} annotation object from the current test method.
     */
    protected Params currentTestParams() {
        Params params = currentTestAnnotation(Params.class);

        assertNotNull("Test " + getName() + " is not annotated with @Param annotation.", params);

        return params;
    }

    /**
     * Assert that two numbers are close to each other.
     */
    private static void assertAlmostEqual(long exp, long actual) {
        assertTrue(String.format("Numbers differ too much [exp=%d, actual=%d]", exp, actual), almostEqual(exp, actual));
    }

    /**
     * Assert that two numbers are not close to each other.
     */
    private static void assertNotAlmostEqual(long exp, long actual) {
        assertFalse(String.format("Numbers are almost equal [exp=%d, actual=%d]", exp, actual), almostEqual(exp, actual));
    }

    /**
     * Check that two numbers are close to each other.
     */
    private static boolean almostEqual(long exp, long actual) {
        double rel = (double)(actual - exp) / exp;

        return Math.abs(rel) < 0.05;
    }
}
