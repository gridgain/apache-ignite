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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * DHT atomic cache entry.
 */
public class GridDhtAtomicCacheEntry extends GridDhtCacheEntry {
    /**
     * @param ctx Cache context.
     * @param topVer Topology version at the time of creation (if negative, then latest topology is assumed).
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header id.
     */
    public GridDhtAtomicCacheEntry(GridCacheContext ctx,
        long topVer,
        KeyCacheObject key,
        int hash,
        CacheObject val,
        GridCacheMapEntry next,
        long ttl,
        int hdrId)
    {
        super(ctx, topVer, key, hash, val, next, ttl, hdrId);
    }

    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return CU.isNearEnabled(cctx) ? super.cacheName() : cctx.dht().name();
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridDhtAtomicCacheEntry.class, this, super.toString());
    }
}
