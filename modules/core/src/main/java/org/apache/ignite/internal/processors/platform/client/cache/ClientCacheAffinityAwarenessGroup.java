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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Partition mapping associated with the group of caches.
 */
class ClientCacheAffinityAwarenessGroup {
    /** Binary processor. */
    CacheObjectBinaryProcessorImpl proc;

    /** Partitions map for caches. */
    private final HashMap<UUID, Set<Integer>> partitionsMap;

    /** IDs of the groups of the associated caches. */
    private HashSet<Integer> cacheGroupIds;

    /** Descriptor of the associated caches. */
    private HashMap<Integer, CacheConfiguration> cacheCfgs;

    /**
     * @param ctx Connection context.
     * @param cacheDesc Descriptor of the initial cache.
     * @param affVer Affinity topology version.
     */
    public ClientCacheAffinityAwarenessGroup(ClientConnectionContext ctx, DynamicCacheDescriptor cacheDesc,
        AffinityTopologyVersion affVer) {
        proc = (CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects();

        int cacheId = cacheDesc.cacheId();
        CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

        boolean applicable = isApplicable(ccfg);
        partitionsMap = !applicable ? null : getPartitionsMap(ctx, cacheId, affVer);

        cacheCfgs = new HashMap<>();
        cacheCfgs.put(cacheId, ccfg);

        cacheGroupIds = new HashSet<>();
        cacheGroupIds.add(cacheDesc.groupId());
    }

    /**
     * Adds caches from the other mapping to current if they are compatible.
     * @param another Another mapping.
     * @return True if merged.
     */
    public boolean tryMerge(ClientCacheAffinityAwarenessGroup another) {
        if (isCompatible(another)) {
            cacheCfgs.putAll(another.cacheCfgs);
            cacheGroupIds.addAll(another.cacheGroupIds);

            return true;
        }

        return false;
    }

    /**
     * Check if the mapping is compatible to another one.
     * @param another Another mapping.
     * @return True if compatible.
     */
    private boolean isCompatible(ClientCacheAffinityAwarenessGroup another) {
        // All unapplicable caches go to the same single group, so they are all compatible one to another.
        if (partitionsMap == null || another.partitionsMap == null)
            return partitionsMap == another.partitionsMap;

        // Checking groups for fast results. If cache group sets intersect then mappings are the same.
        for (int cacheGroupId : another.cacheGroupIds) {
            if (cacheGroupIds.contains(cacheGroupId))
                return true;
        }

        // Now we need to compare mappings themselves.
        return partitionsMap.equals(another.partitionsMap);
    }

    /**
     * @param ccfg Cache configuration.
     * @return True if cache is applicable for affinity awareness optimisation.
     */
    private static boolean isApplicable(CacheConfiguration ccfg) {
        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return false;

        // Only caches with no custom affinity key mapper is supported.
        AffinityKeyMapper keyMapper = ccfg.getAffinityMapper();
        if (!(keyMapper instanceof CacheDefaultBinaryAffinityKeyMapper))
            return false;

        // Only RendezvousAffinityFunction is supported for now.
        if (!ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class))
            return false;

        IgnitePredicate filter = ccfg.getNodeFilter();
        boolean hasNodeFilter = filter != null && !(filter instanceof CacheConfiguration.IgniteAllNodesPredicate);

        // We cannot be sure that two caches are co-located if custom node filter is present.
        // Note that technically we may try to compare two filters. However, this adds unnecessary complexity
        // and potential deserialization issues.
        return !hasNodeFilter;
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        writer.writeBoolean(partitionsMap != null);

        writer.writeInt(cacheCfgs.size());

        for (Map.Entry<Integer, CacheConfiguration> entry: cacheCfgs.entrySet()) {
            writer.writeInt(entry.getKey());

            if (partitionsMap == null)
                continue;

            CacheConfiguration ccfg = entry.getValue();
            CacheKeyConfiguration[] keyCfgs = ccfg.getKeyConfiguration();

            if (keyCfgs == null) {
                writer.writeInt(0);

                continue;
            }

            writer.writeInt(keyCfgs.length);

            for (CacheKeyConfiguration keyCfg : keyCfgs) {
                int keyTypeId = proc.typeId(keyCfg.getTypeName());
                int affinityKeyFieldId = proc.binaryContext().fieldId(keyTypeId, keyCfg.getAffinityKeyFieldName());

                writer.writeInt(keyTypeId);
                writer.writeInt(affinityKeyFieldId);
            }
        }

        if (partitionsMap == null)
            return;

        writer.writeInt(partitionsMap.size());

        for (HashMap.Entry<UUID, Set<Integer>> nodeParts: partitionsMap.entrySet()) {
            UUID nodeUuid = nodeParts.getKey();
            Set<Integer> parts = nodeParts.getValue();

            writer.writeUuid(nodeUuid);

            writer.writeInt(parts.size());
            for (int part : parts)
                writer.writeInt(part);
        }
    }

    /**
     * Get partition map for a cache.
     * @param ctx Connection context.
     * @param cacheId Cache ID.
     * @return Partitions mapping for cache.
     */
    private static HashMap<UUID, Set<Integer>> getPartitionsMap(ClientConnectionContext ctx, int cacheId,
        AffinityTopologyVersion affVer) {

        GridCacheContext cacheContext = ctx.kernalContext().cache().context().cacheContext(cacheId);
        AffinityAssignment assignment = cacheContext.affinity().assignment(affVer);
        Set<ClusterNode> nodes = assignment.primaryPartitionNodes();

        HashMap<UUID, Set<Integer>> res = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            UUID nodeId = node.id();
            Set<Integer> parts = assignment.primaryPartitions(nodeId);

            res.put(nodeId, parts);
        }

        return res;
    }
}
