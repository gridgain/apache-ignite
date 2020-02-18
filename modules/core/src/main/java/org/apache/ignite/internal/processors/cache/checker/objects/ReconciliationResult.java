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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Common result of partition reconciliation.
 */
public class ReconciliationResult extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     * Result.
     */
    private PartitionReconciliationResult partitionReconciliationResult;

    /**
     * Folders with local results.
     */
    private Map<UUID, String> nodeIdToFolder;

    /**
     * Errors happened during execution.
     */
    private List<String> errors;

    /**
     * Default constructor.
     */
    public ReconciliationResult() {
    }

    /**
     * @param partReconciliationRes Partition reconciliation response.
     * @param nodeIdToFolder Node id to folder.
     * @param errors Errors.
     */
    public ReconciliationResult(
        PartitionReconciliationResult partReconciliationRes,
        Map<UUID, String> nodeIdToFolder,
        List<String> errors
    ) {
        this.partitionReconciliationResult = partReconciliationRes;
        this.nodeIdToFolder = nodeIdToFolder;
        this.errors = errors;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(partitionReconciliationResult);
        U.writeMap(out, nodeIdToFolder);
        U.writeCollection(out, errors);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        partitionReconciliationResult = (PartitionReconciliationResult)in.readObject();

        nodeIdToFolder = U.readMap(in);

        errors = U.readList(in);
    }

    /**
     * @return Result.
     */
    public PartitionReconciliationResult partitionReconciliationResult() {
        return partitionReconciliationResult;
    }

    /**
     * @return Folders with local results.
     */
    public Map<UUID, String> nodeIdToFolder() {
        return nodeIdToFolder;
    }

    /**
     * @return Errors happened during execution.
     */
    public List<String> errors() {
        return errors;
    }
}
