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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Historic WAL rebalance base test.
 */
public class IgniteWalRebalanceTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0"); //to make all rebalance wal-based

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

        ccfg.setCacheMode(CacheMode.REPLICATED);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
                    .setWalHistorySize(Integer.MAX_VALUE)
                    .setWalMode(WALMode.LOG_ONLY)
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setCommunicationSpi(new WalRebalanceCheckingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test simple WAL historical rebalance.
     *
     * @throws Exception if failed.
     */
    public void testSimple() throws Exception {
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);
        final int entryCnt = 10_000;

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        stopGrid(1, false);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k + 1));

        forceCheckpoint();

        ig1 = startGrid(1);

        awaitPartitionMapExchange();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                assertEquals(new IndexedObject(k + 1), cache1.get(k));
        }
    }

    /**
     * Test that cache entry removes are rebalanced properly using WAL.
     *
     * @throws Exception If failed.
     */
    public void testRebalanceRemoves() throws Exception {
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);
        final int entryCnt = 10_000;

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        stopGrid(1, false);

        for (int k = 0; k < entryCnt; k++) {
            if (k % 3 != 2)
                cache.put(k, new IndexedObject(k + 1));
            else // Spread removes across all partitions.
                cache.remove(k);
        }

        forceCheckpoint();

        ig1 = startGrid(1);

        awaitPartitionMapExchange();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++) {
                if (k % 3 != 2)
                    assertEquals(new IndexedObject(k + 1), cache1.get(k));
                else
                    assertNull(cache1.get(k));
            }
        }
    }

    /**
     * Test that cache entry removes are rebalanced properly using WAL.
     *
     * @throws Exception If failed.
     */
    public void testWalHistoryWorksCorrectly() throws Exception {
        final int entryCnt = 10_000;

        IgniteEx ig0 = (IgniteEx) startGrids(2);

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        // This node should rebalance data from other nodes and shouldn't have WAL history.
        Ignite ignite = startGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        Set<Long> topVers = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions();

        Assert.assertTrue(topVers.contains(ignite.cluster().topologyVersion()));

        // Rewrite some data.
        for (int k = 0; k < entryCnt; k++) {
            if (k % 3 == 0)
                cache.put(k, new IndexedObject(k + 1));
            else if (k % 3 == 1) // Spread removes across all partitions.
                cache.remove(k);
        }

        // Stop grids which have actual WAL history.
        stopGrid(0);

        stopGrid(1);

        // Start new node which should rebalance all data from node(2) without using WAL.
        ignite = startGrid(3);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        Set<Long> topVers2 = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions();

        Assert.assertFalse(topVers2.contains(ignite.cluster().topologyVersion()));

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++) {
                if (k % 3 == 0)
                    assertEquals(new IndexedObject(k + 1), cache1.get(k));
                else if (k % 3 == 1)
                    assertNull(cache1.get(k));
                else
                    assertEquals(new IndexedObject(k), cache1.get(k));
            }
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        private byte[] payload = new byte[1024];

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }

    /**
     * Wrapper of communication spi to detect on what topology versions WAL rebalance has happened.
     */
    public static class WalRebalanceCheckingCommunicationSpi extends TcpCommunicationSpi {
        /** Topology vers. */
        private final Set<Long> topVers = new GridConcurrentHashSet<>();

        /**
         *
         */
        public Set<Long> walRebalanceVersions() {
            return Collections.unmodifiableSet(topVers);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) ((GridIoMessage)msg).message();

                IgniteDhtDemandedPartitionsMap map = demandMsg.partitions();

                if (!map.historicalMap().isEmpty())
                    topVers.add(demandMsg.topologyVersion().topologyVersion());
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}