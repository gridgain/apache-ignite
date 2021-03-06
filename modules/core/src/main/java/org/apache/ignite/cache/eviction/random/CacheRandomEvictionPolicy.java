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

package org.apache.ignite.cache.eviction.random;

import org.apache.ignite.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.*;

/**
 * Cache eviction policy which will select random cache entry for eviction if cache
 * size exceeds the {@link #getMaxSize()} parameter. This implementation is
 * extremely light weight, lock-free, and does not create any data structures to maintain
 * any order for eviction.
 * <p>
 * Random eviction will provide the best performance over any key set in which every
 * key has the same probability of being accessed.
 */
public class CacheRandomEvictionPolicy<K, V> implements CacheEvictionPolicy<K, V>,
    CacheRandomEvictionPolicyMBean {
    /** Maximum size. */
    private volatile int max = CacheConfiguration.DFLT_CACHE_SIZE;

    /**
     * Constructs random eviction policy with all defaults.
     */
    public CacheRandomEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructs random eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public CacheRandomEvictionPolicy(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry) {
        if (!entry.isCached())
            return;

        IgniteCache<K, V> cache = entry.unwrap(IgniteCache.class);

        int size = cache.size();

        for (int i = max; i < size; i++) {
            Cache.Entry<K, V> e = cache.randomEntry();

            if (e != null)
                e.unwrap(EvictableEntry.class).evict();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheRandomEvictionPolicy.class, this);
    }
}
