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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DHT atomic cache entry for off-heap tiered or off-heap values modes.
 */
public class GridDhtAtomicOffHeapCacheEntry extends GridDhtAtomicCacheEntry {
    /** Off-heap value pointer. */
    private long valPtr;

    /**
     * @param ctx    Cache context.
     * @param topVer Topology version at the time of creation (if negative, then latest topology is assumed).
     * @param key    Cache key.
     * @param hash   Key hash value.
     * @param val    Entry value.
     */
    public GridDhtAtomicOffHeapCacheEntry(
        GridCacheContext ctx,
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        int hash,
        CacheObject val
    ) {
        super(ctx, topVer, key, hash, val);
    }

    /** {@inheritDoc} */
    @Override protected boolean hasOffHeapPointer() {
        return valPtr != 0;
    }

    /** {@inheritDoc} */
    @Override protected long offHeapPointer() {
        return valPtr;
    }

    /** {@inheritDoc} */
    @Override protected void offHeapPointer(long valPtr) {
        this.valPtr = valPtr;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridDhtAtomicOffHeapCacheEntry.class, this,
            "entry", System.identityHashCode(this),
            "super", super.toString());
    }
}
