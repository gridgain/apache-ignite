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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;

/**
 * Helper class for ImpurityHistograms.
 */
public abstract class ImpurityHistogram {
    /** Bucket ids. */
    protected final Set<Integer> bucketIds = new TreeSet<>();

    /** Feature id. */
    protected final int featureId;

    public ImpurityHistogram(int featureId) {
        this.featureId = featureId;
    }

    protected Optional<NodeSplit> checkAndReturnSplitValue(int bestBucketId, double bestSplitVal, double bestImpurity) {
        if (isLastBucket(bestBucketId))
            return Optional.empty();
        else
            return Optional.of(new NodeSplit(featureId, bestSplitVal, bestImpurity));
    }

    /**
     * @param bestBucketId Best bucket id.
     * @return true if best found bucket is last within all bucketIds.
     */
    private boolean isLastBucket(int bestBucketId) {
        int minBucketId = Integer.MAX_VALUE;
        int maxBucketId = Integer.MIN_VALUE;
        for (Integer bucketId : bucketIds) {
            minBucketId = Math.min(minBucketId, bucketId);
            maxBucketId = Math.max(maxBucketId, bucketId);
        }

        return bestBucketId == maxBucketId;
    }
}
