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

package org.apache.ignite.spi.checkpoint.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Test for cache checkpoint SPI with second cache configured.
 */
public class CacheCheckpointSpiSecondCacheSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Data cache name. */
    private static final String DATA_CACHE = null;

    /** Checkpoints cache name. */
    private static final String CP_CACHE = "checkpoints";

    /** Starts grid. */
    public CacheCheckpointSpiSecondCacheSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName(DATA_CACHE);
        cacheCfg1.setCacheMode(REPLICATED);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);

        CacheConfiguration cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName(CP_CACHE);
        cacheCfg2.setCacheMode(REPLICATED);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        CacheCheckpointSpi cp = new CacheCheckpointSpi();

        cp.setCacheName(CP_CACHE);

        cfg.setCheckpointSpi(cp);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSecondCachePutRemove() throws Exception {
        IgniteCache<Integer, Integer> data = grid().jcache(DATA_CACHE);
        IgniteCache<Integer, String> cp = grid().jcache(CP_CACHE);

        data.put(1, 1);
        cp.put(1, "1");

        Integer v = data.get(1);

        assertNotNull(v);
        assertEquals(Integer.valueOf(1), data.get(1));

        data.remove(1);

        assertNull(data.get(1));

        assertTrue(data.localSize() == 0);

        assertEquals(1, cp.size());
        assertEquals("1", cp.get(1));

    }
}
