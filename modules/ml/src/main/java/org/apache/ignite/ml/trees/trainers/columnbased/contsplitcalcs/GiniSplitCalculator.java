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

package org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs;

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import java.util.DoubleSummaryStatistics;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.ContinuousSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SampleInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.vectors.SplitInfo;

public class GiniSplitCalculator implements ContinuousSplitCalculator<GiniSplitCalculator.GiniData> {
    @Override public GiniData calculateRegionInfo(DoubleStream s, int l) {
        Double2IntOpenHashMap m = new Double2IntOpenHashMap();

        s.sequential().forEach(k -> m.compute(k, (a, i) -> a != null ? i + 1 : 0));

        int size = m.size();

        DoubleSummaryStatistics stat = m.values().stream().mapToDouble(v -> (double)v / size * (1 - (double)v / size)).summaryStatistics();

        return new GiniData(stat.getSum(), l, l + (int)stat.getCount(), m);
    }

    @Override public SplitInfo<GiniData> splitRegion(Stream<SampleInfo> s, int regionIdx,
        GiniData data) {
        // TODO: Implement
        throw new UnsupportedOperationException();
    }

    public class GiniData extends ContinuousRegionInfo {
        private Double2IntOpenHashMap m;

        /** {@inheritDoc} */
        public GiniData(double impurity, int left, int right, Double2IntOpenHashMap m) {
            super(impurity, left, right);
            this.m = m;
        }
    }
}
