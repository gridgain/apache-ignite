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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Metrics test for partitioned cache with disabled near cache.
 */
public class GridCachePartitionedNearDisabledMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(gridCount() - 1);
        cfg.setRebalanceMode(SYNC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setDistributionMode(PARTITIONED_ONLY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    // TODO: extend from GridCacheTransactionalAbstractMetricsSelfTest and uncomment:

//    /** {@inheritDoc} */
//    @Override protected int expectedReadsPerPut(boolean isPrimary) {
//        return 1;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int expectedMissesPerPut(boolean isPrimary) {
//        return 1;
//    }

    /**
     * @throws Exception If failed.
     */
    public void _testGettingRemovedKey() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        cache.put(0, 0);

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            // TODO: getting of removed key will produce 3 inner read operations.
            g.jcache(null).removeAll();

            // TODO: getting of removed key will produce inner write and 4 inner read operations.
            //((IgniteKernal)g).cache(null).remove(0);

            assert g.jcache(null).localSize() == 0;

            g.jcache(null).mxBean().clear();
        }

        assertNull("Value is not null for key: " + 0, cache.get(0));

        // Check metrics for the whole cache.
        long removes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).jcache(null).metrics();

            removes += m.getCacheRemovals();
            reads += m.getCacheGets();
            hits += m.getCacheHits();
            misses += m.getCacheMisses();
        }

        assertEquals(0, removes);
        assertEquals(1, reads);
        assertEquals(0, hits);
        assertEquals(1, misses);
    }
}
