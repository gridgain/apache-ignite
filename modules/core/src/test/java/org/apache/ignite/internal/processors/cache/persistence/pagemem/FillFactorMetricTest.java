/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for fillFactor metrics.
 */
public class FillFactorMetricTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String MY_DATA_REGION = "MyPolicy";

    /** */
    private static final String MY_CACHE = "mycache";

    /** */
    public static final int NODES = 2;

    /** */
    public static final long LARGE_PRIME = 4294967291L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setMemoryConfiguration(
                new MemoryConfiguration().setMemoryPolicies(
                    new MemoryPolicyConfiguration()
                        .setName(MY_DATA_REGION)
                        .setInitialSize(100 * 1024L * 1024L)
                        .setMaxSize(200 * 1024L * 1024L)
                        .setMetricsEnabled(true)
                ));
    }

    /** */
    protected CacheConfiguration<Object, Object> cacheCfg() {
        return new CacheConfiguration<>()
            .setName(MY_CACHE)
            .setMemoryPolicyName(MY_DATA_REGION)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(16));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Records counter.
     */
    private AtomicInteger recordsInCache = new AtomicInteger();

    /**
     * Last fill factor values.
     */
    private final float[] curFillFactor = new float[NODES];

    /**
     * throws if failed.
     */
    public void testFillAndEmpty() throws Exception {
        final AtomicBoolean stopLoadFlag = new AtomicBoolean();
        final AtomicBoolean doneFlag = new AtomicBoolean();

        startGrids(NODES);

        grid(0).getOrCreateCache(cacheCfg());

        final int pageSize = grid(0).configuration().getMemoryConfiguration().getPageSize();

        IgniteInternalFuture printStatFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                while (!doneFlag.get()) {
                    log.info("Stat nodes:");
                    printStat(0);
                    printStat(1);

                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        return;
                    }
                }
            }

            protected void printStat(int node) {
                MemoryMetrics m = grid(node).memoryMetrics(MY_DATA_REGION);

                float fillFactor = m.getPagesFillFactor();

                long usedMem = (long)((m.getPhysicalMemoryPages() * pageSize)
                        * fillFactor);

                log.info(String.format("Stat node-%d:\t%d\t%f\t%d",
                    node,
                    m.getPhysicalMemoryPages(),
                    fillFactor,
                    usedMem
                ));

                curFillFactor[node] = fillFactor;
            }
        });

        for (int iter = 0; iter < 3; iter++) {
            log.info("Going upward");

            stopLoadFlag.set(false);
            recordsInCache.set(0);

            IgniteInternalFuture loadFut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    IgniteCache<Object, Object> cache = grid(0).cache(MY_CACHE);

                    while (!stopLoadFlag.get()) {
                        int i = recordsInCache.incrementAndGet();

                        final long res = (i * i) % LARGE_PRIME;

                        cache.put(res, new byte[1 << (res % 16)]);

                        try {
                            // Steadily add entries to cache but avoid overconsumption of RAM and CPU
                            Thread.sleep(1);
                        }
                        catch (InterruptedException ie) {
                            return;
                        }
                    }
                }
            });

            // Wait for cache to be reasonably full
            U.sleep(6_000);

            stopLoadFlag.set(true);

            loadFut.get();

            // Fill factor will typically be 0.98
            for (float fillFactor : curFillFactor)
                assertTrue("FillFactor too low: " + fillFactor, fillFactor > 0.9);

            log.info("Going downward");

            IgniteInternalFuture clearFut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    IgniteCache<Object, Object> cache = grid(0).cache(MY_CACHE);

                    int i;
                    while ((i = recordsInCache.getAndDecrement()) > 0) {
                        final long res = (i * i) % LARGE_PRIME;

                        cache.remove(res);

                        try {
                            Thread.sleep(1);
                        }
                        catch (InterruptedException ie) {
                            return;
                        }
                    }
                }
            });

            // Wait for cache to be cleared
            clearFut.get();

            // Fill factor will typically be 0.8, occupied pages mostly partition metadata
            for (float fillFactor : curFillFactor)
                assertTrue("FillFactor too high: " + fillFactor, fillFactor < 0.85);
        }

        doneFlag.set(true);

        printStatFut.get();
    }
}
