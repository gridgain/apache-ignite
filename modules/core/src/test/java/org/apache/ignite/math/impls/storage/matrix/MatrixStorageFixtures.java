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

package org.apache.ignite.math.impls.storage.matrix;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.ignite.math.MatrixStorage;

import static org.apache.ignite.math.StorageConstants.*;

/**
 *
 */
class MatrixStorageFixtures {
    private static final List<Supplier<Iterable<MatrixStorage>>> suppliers = Arrays.asList(
        new Supplier<Iterable<MatrixStorage>>() {
            @Override public Iterable<MatrixStorage> get() {
                return new SparseLocalMatrixStorageFixture();
            }
        }
    );

    void consumeSampleStorages(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<MatrixStorage, String> consumer){
        for (Supplier<Iterable<MatrixStorage>> fixtureSupplier : suppliers) {
            final Iterable<MatrixStorage> fixture = fixtureSupplier.get();

            for (MatrixStorage matrixStorage : fixture) {
                if (paramsConsumer != null)
                    paramsConsumer.accept(matrixStorage.rowSize(), matrixStorage.columnSize());

                consumer.accept(matrixStorage, fixture.toString());
            }
        }
    }

    private static class SparseLocalMatrixStorageFixture implements Iterable<MatrixStorage>{
        private final Integer[] rows = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 512, 1024, null};
        private final Integer[] cols = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 512, null};
        private final Integer[] randomAccess = new Integer[] {SEQUENTIAL_ACCESS_MODE, RANDOM_ACCESS_MODE, null};
        private final Integer[] rowStorage = new Integer[] {ROW_STORAGE_MODE, COLUMN_STORAGE_MODE, null};
        private int sizeIdx = 0;
        private int acsModeIdx = 0;
        private int stoModeIdx = 0;

        @Override public Iterator<MatrixStorage> iterator() {
            return new Iterator<MatrixStorage>() {
                @Override public boolean hasNext() {
                    return hasNextCol(sizeIdx) && hasNextRow(sizeIdx)
                        && hasNextAcsMode(acsModeIdx) && hasNextStoMode(stoModeIdx);
                }

                @Override public MatrixStorage next() {
                    if (!hasNext())
                        throw new NoSuchElementException(SparseLocalMatrixStorageFixture.this.toString());

                    MatrixStorage storage = new SparseLocalOnHeapMatrixStorage(
                        rows[sizeIdx], cols[sizeIdx], randomAccess[acsModeIdx], rowStorage[stoModeIdx]);

                    nextIdx();

                    return storage;
                }

                private void nextIdx(){
                    if (hasNextStoMode(stoModeIdx + 1)){
                        stoModeIdx++;

                        return;
                    }

                    stoModeIdx = 0;

                    if (hasNextAcsMode(acsModeIdx + 1)){
                        acsModeIdx++;

                        return;
                    }

                    acsModeIdx = 0;
                    sizeIdx++;
                }
            };
        }

        @Override public String toString() {
            return "SparseLocalMatrixStorageFixture{ " + "rows=" + rows[sizeIdx] + ", cols=" + cols[sizeIdx] +
                ", access mode=" + randomAccess[acsModeIdx] + ", storage mode=" + rowStorage[stoModeIdx] + "}";
        }

        private boolean hasNextRow(int idx){
            return rows[idx] != null;
        }

        private boolean hasNextCol(int idx){
            return cols[idx] != null;
        }

        private boolean hasNextAcsMode(int idx){
            return randomAccess[idx] != null;
        }

        private boolean hasNextStoMode(int idx){
            return rowStorage[idx] != null;
        }
    }
}
