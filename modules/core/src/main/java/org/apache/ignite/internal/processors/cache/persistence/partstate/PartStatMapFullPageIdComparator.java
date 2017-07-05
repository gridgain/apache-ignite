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

package org.apache.ignite.internal.processors.cache.persistence.partstate;

import java.io.Serializable;
import java.util.Comparator;

/**
 *
 */
public class PartStatMapFullPageIdComparator implements Comparator<CachePartitionId>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final PartStatMapFullPageIdComparator INSTANCE = new PartStatMapFullPageIdComparator();

    /** {@inheritDoc} (cacheId, partition) */
    @Override public int compare(CachePartitionId o1, CachePartitionId o2) {
        if (o1.getCacheId() < o2.getCacheId())
            return -1;

        if (o1.getCacheId() > o2.getCacheId())
            return 1;

        if (o1.getPartId() < o2.getPartId())
            return -1;

        if (o1.getPartId() > o2.getPartId())
            return 1;

        return 0;
    }
}

