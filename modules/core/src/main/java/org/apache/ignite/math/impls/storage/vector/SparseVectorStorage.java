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

package org.apache.ignite.math.impls.storage.vector;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.ignite.math.*;
import java.io.*;
import java.util.*;

/**
 * Vector storage that stores only non-zero doubles.
 */
public class SparseVectorStorage implements VectorStorage {
    private int size;
    private boolean seqAcs; // 'True' for sequential access, 'false' for random access.

    // Actual map storage.
    private Map<Integer, Double> sto;

    public SparseVectorStorage() {
        // No-op.
    }

    /**
     *
     * @param size
     * @param seqAcs
     */
    public SparseVectorStorage(int size, boolean seqAcs) {
        this.size  = size;
        this.seqAcs = seqAcs;

        initStorage();
    }

    /**
     *
     */
    private void initStorage() {
        if (seqAcs)
            sto = new Int2DoubleRBTreeMap();
        else
            sto = new Int2DoubleOpenHashMap();
    }

    /**
     * 
     * @return
     */
    public boolean isRandomAccess() {
        return !seqAcs;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public double get(int i) {
        return sto.get(i);
    }

    @Override
    public void set(int i, double v) {
        sto.put(i, v);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeBoolean(seqAcs);
        out.writeObject(sto);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        seqAcs = in.readBoolean();
        sto = (Map<Integer, Double>)in.readObject();
    }

    @Override
    public boolean isSequentialAccess() {
        return seqAcs;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public double getLookupCost() {
        return 0;
    }

    @Override
    public boolean isAddConstantTime() {
        return true;
    }

    @Override
    public boolean isArrayBased() {
        return false;
    }
}
