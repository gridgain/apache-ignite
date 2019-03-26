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

package org.apache.ignite.internal.jdbc.thin;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;

public final class AffinityCache {
    /** Partition distributions cache limit. */
    public static final int DISTRIBUTIONS_CACHE_LIMIT = 1000;

    /** SQL cache limit. */
    public static final int SQL_CACHE_LIMIT = 100_000;

    private final AffinityTopologyVersion version;

    private final GridBoundedLinkedHashMap<Integer, Map<Integer, UUID>> cachePartitionsDistribution;

    private final GridBoundedLinkedHashMap<String, JdbcThinPartitionResult> sqlCache;

    public AffinityCache(AffinityTopologyVersion version) {
        this.version = version;

        cachePartitionsDistribution = new GridBoundedLinkedHashMap<>(DISTRIBUTIONS_CACHE_LIMIT);

        sqlCache = new GridBoundedLinkedHashMap<>(SQL_CACHE_LIMIT);
    }

    /**
     * @return Version.
     */
    public AffinityTopologyVersion version() {
        return version;
    }

    void addCacheDistribution(Integer cacheId, Map<Integer, UUID> distribution) {
        cachePartitionsDistribution.put(cacheId, distribution);
    }

    void addSqlQuery(String sql, JdbcThinPartitionResult partRes) {
        sqlCache.put(sql, partRes);
    }

    JdbcThinPartitionResult partitionResult(String sqlQry) {
        return sqlCache.get(sqlQry);
    }

    boolean containsPartitionResult(String sqlQry) {
        return sqlCache.containsKey(sqlQry);
    }

    Map<Integer, UUID> cacheDistribution(int cacheId) {
        return cachePartitionsDistribution.get(cacheId);
    }
}
