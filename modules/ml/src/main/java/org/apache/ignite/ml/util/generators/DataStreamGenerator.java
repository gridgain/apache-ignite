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

package org.apache.ignite.ml.util.generators;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducer;

/**
 * Provides general interface for generation of pseudorandom vectors in according to shape defined
 * by logic of specific data stream generator.
 */
public interface DataStreamGenerator {
    /**
     * @return stream of {@link LabeledVector} in according to dataset shape.
     */
    public Stream<LabeledVector<Vector, Double>> labeled();

    /**
     * @return stream of unlabeled {@link Vector} in according to dataset shape.
     */
    public default Stream<Vector> unlabeled() {
        return labeled().map(DatasetRow::features);
    }

    /**
     * @param classifier user defined classifier for vectors stream.
     * @return stream of {@link LabeledVector} in according to dataset shape and user's classifier.
     */
    public default Stream<LabeledVector<Vector, Double>> labeled(IgniteFunction<Vector, Double> classifier) {
        return labeled().map(DatasetRow::features).map(v -> new LabeledVector<>(v, classifier.apply(v)));
    }

    /**
     * Apply user defined mapper to vectors stream without labels hiding.
     *
     * @param f mapper of vectors of data stream.
     * @return stream of mapped vectors.
     */
    public default DataStreamGenerator mapVectors(IgniteFunction<Vector, Vector> f) {
        DataStreamGenerator orig = this;
        return new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Vector, Double>> labeled() {
                return orig.labeled().map(v -> new LabeledVector<>(f.apply(v.features()), v.label()));
            }
        };
    }

    /**
     * @param rnd generator of pseudorandom scalars modifying vector components with label saving.
     * @return stream of blurred vectors with same labels.
     */
    public default DataStreamGenerator blur(RandomProducer rnd) {
        return mapVectors(rnd::noizify);
    }

    /**
     * Convert first N values from stream to map.
     *
     * @param datasetSize dataset size.
     * @return map of vectors and labels.
     */
    public default Map<Vector, Double> asMap(int datasetSize) {
        return labeled().limit(datasetSize)
            .collect(Collectors.toMap(DatasetRow::features, LabeledVector::label));
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize dataset size.
     * @param partitions partitions count.
     * @return dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, int partitions) {
        return new DatasetBuilderAdapter(this, datasetSize, partitions);
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize dataset size.
     * @param filter data filter.
     * @param partitions partitions count.
     * @return dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, IgniteBiPredicate<Vector, Double> filter,
        int partitions) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions);
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize dataset size.
     * @param filter data filter.
     * @param partitions partitions count.
     * @param upstreamTransformerBuilder upstream transformer builder.
     * @return dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, IgniteBiPredicate<Vector, Double> filter,
        int partitions, UpstreamTransformerBuilder<Vector, Double> upstreamTransformerBuilder) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions, upstreamTransformerBuilder);
    }

}
