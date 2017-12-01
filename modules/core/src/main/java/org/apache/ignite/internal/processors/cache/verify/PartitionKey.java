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
package org.apache.ignite.internal.processors.cache.verify;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition key - pair of cache group ID and partition ID.
 */
public class PartitionKey {
    /** Group id. */
    private int grpId;

    /** Partition id. */
    private int partId;

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     */
    public PartitionKey(int grpId, int partId) {
        this.grpId = grpId;
        this.partId = partId;
    }

    /**
     * @return Group id.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Partition id.
     */
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PartitionKey key = (PartitionKey)o;

        return grpId == key.grpId && partId == key.partId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = grpId;

        res = 31 * res + partId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionKey.class, this);
    }
}
