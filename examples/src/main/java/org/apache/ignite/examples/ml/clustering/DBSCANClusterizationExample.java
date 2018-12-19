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

package org.apache.ignite.examples.ml.clustering;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.clustering.dbscan.DBSCANModel;
import org.apache.ignite.ml.clustering.dbscan.DBSCANTrainer2;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Run KMeans clustering algorithm ({@link KMeansTrainer}) over distributed dataset.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * After that it trains the model based on the specified data using
 * <a href="https://en.wikipedia.org/wiki/K-means_clustering">KMeans</a> algorithm.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict what cluster
 * does this point belong to, and compares prediction to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class DBSCANClusterizationExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> DBSCAN clustering algorithm over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, double[]> dataCache = getTestCache(ignite);
            long startTime = System.currentTimeMillis();
            System.out.println("Start training: " + startTime);

            DBSCANTrainer2 trainer = new DBSCANTrainer2()
                .withSeed(7867L);

            DBSCANModel mdl = trainer.fit(
                ignite,
                dataCache,
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
                (k, v) -> v[0]
            );

            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("Finish training for: " + totalTime);

       /*     System.out.println(">>> KMeans centroids");
            System.out.println(">>>");

            System.out.println(">>> --------------------------------------------");
            System.out.println(">>> | Predicted cluster\t| Erased class label\t|");
            System.out.println(">>> --------------------------------------------");

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    //Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);
                   // int clusterId = ((DBSCANVector)val).clusterId;
                  //  String status = ((DBSCANVector)val).status.toString();
                   // double prediction = mdl.apply(inputs);

                 //   System.out.printf(">>> | %.4f\t\t\t| %.4f\t\t| %.4f\t\t|\n", clusterId, groundTruth, status);
                }

                System.out.println(">>> ---------------------------------");
                System.out.println(">>> KMeans clustering algorithm over cached dataset usage example completed.");
            }*/
        }
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param ignite Ignite instance.
     * @return Filled Ignite Cache.
     */
    private static IgniteCache<Integer, double[]> getTestCache(Ignite ignite) {
        CacheConfiguration<Integer, double[]> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 16));

        IgniteCache<Integer, double[]> cache = ignite.createCache(cacheConfiguration);

        for (int k = 0; k < 100; k++) { // multiplies the Iris dataset k times.
            for (int i = 0; i < data.length; i++)
                cache.put(k * 10000 + i, mutate(data[i], k));
        }

        return cache;
    }

    /**
     * Tiny changing of data depending on k parameter.
     *
     * @param datum The vector data.
     * @param k The passed parameter.
     * @return The changed vector data.
     */
    private static double[] mutate(double[] datum, int k) {
        for (int i = 1; i < datum.length; i++)
            datum[i] += (double)k / 1_000_000;
        return datum;
    }

    /** The Iris dataset. */
    private static final double[][] data = {
        {1, 5.1, 3.5, 1.4, 0.2},
        {1, 4.9, 3, 1.4, 0.2},
        {1, 4.7, 3.2, 1.3, 0.2},
        {1, 4.6, 3.1, 1.5, 0.2},
        {1, 5, 3.6, 1.4, 0.2},
        {1, 5.4, 3.9, 1.7, 0.4},
        {1, 4.6, 3.4, 1.4, 0.3},
        {1, 5, 3.4, 1.5, 0.2},
        {1, 4.4, 2.9, 1.4, 0.2},
        {1, 4.9, 3.1, 1.5, 0.1},
        {1, 5.4, 3.7, 1.5, 0.2},
        {1, 4.8, 3.4, 1.6, 0.2},
        {1, 4.8, 3, 1.4, 0.1},
        {1, 4.3, 3, 1.1, 0.1},
        {1, 5.8, 4, 1.2, 0.2},
        {1, 5.7, 4.4, 1.5, 0.4},
        {1, 5.4, 3.9, 1.3, 0.4},
        {1, 5.1, 3.5, 1.4, 0.3},
        {1, 5.7, 3.8, 1.7, 0.3},
        {1, 5.1, 3.8, 1.5, 0.3},
        {1, 5.4, 3.4, 1.7, 0.2},
        {1, 5.1, 3.7, 1.5, 0.4},
        {1, 4.6, 3.6, 1, 0.2},
        {1, 5.1, 3.3, 1.7, 0.5},
        {1, 4.8, 3.4, 1.9, 0.2},
        {1, 5, 3, 1.6, 0.2},
        {1, 5, 3.4, 1.6, 0.4},
        {1, 5.2, 3.5, 1.5, 0.2},
        {1, 5.2, 3.4, 1.4, 0.2},
        {1, 4.7, 3.2, 1.6, 0.2},
        {1, 4.8, 3.1, 1.6, 0.2},
        {1, 5.4, 3.4, 1.5, 0.4},
        {1, 5.2, 4.1, 1.5, 0.1},
        {1, 5.5, 4.2, 1.4, 0.2},
        {1, 4.9, 3.1, 1.5, 0.1},
        {1, 5, 3.2, 1.2, 0.2},
        {1, 5.5, 3.5, 1.3, 0.2},
        {1, 4.9, 3.1, 1.5, 0.1},
        {1, 4.4, 3, 1.3, 0.2},
        {1, 5.1, 3.4, 1.5, 0.2},
        {1, 5, 3.5, 1.3, 0.3},
        {1, 4.5, 2.3, 1.3, 0.3},
        {1, 4.4, 3.2, 1.3, 0.2},
        {1, 5, 3.5, 1.6, 0.6},
        {1, 5.1, 3.8, 1.9, 0.4},
        {1, 4.8, 3, 1.4, 0.3},
        {1, 5.1, 3.8, 1.6, 0.2},
        {1, 4.6, 3.2, 1.4, 0.2},
        {1, 5.3, 3.7, 1.5, 0.2},
        {1, 5, 3.3, 1.4, 0.2},
        {2, 7, 3.2, 4.7, 1.4},
        {2, 6.4, 3.2, 4.5, 1.5},
        {2, 6.9, 3.1, 4.9, 1.5},
        {2, 5.5, 2.3, 4, 1.3},
        {2, 6.5, 2.8, 4.6, 1.5},
        {2, 5.7, 2.8, 4.5, 1.3},
        {2, 6.3, 3.3, 4.7, 1.6},
        {2, 4.9, 2.4, 3.3, 1},
        {2, 6.6, 2.9, 4.6, 1.3},
        {2, 5.2, 2.7, 3.9, 1.4},
        {2, 5, 2, 3.5, 1},
        {2, 5.9, 3, 4.2, 1.5},
        {2, 6, 2.2, 4, 1},
        {2, 6.1, 2.9, 4.7, 1.4},
        {2, 5.6, 2.9, 3.6, 1.3},
        {2, 6.7, 3.1, 4.4, 1.4},
        {2, 5.6, 3, 4.5, 1.5},
        {2, 5.8, 2.7, 4.1, 1},
        {2, 6.2, 2.2, 4.5, 1.5},
        {2, 5.6, 2.5, 3.9, 1.1},
        {2, 5.9, 3.2, 4.8, 1.8},
        {2, 6.1, 2.8, 4, 1.3},
        {2, 6.3, 2.5, 4.9, 1.5},
        {2, 6.1, 2.8, 4.7, 1.2},
        {2, 6.4, 2.9, 4.3, 1.3},
        {2, 6.6, 3, 4.4, 1.4},
        {2, 6.8, 2.8, 4.8, 1.4},
        {2, 6.7, 3, 5, 1.7},
        {2, 6, 2.9, 4.5, 1.5},
        {2, 5.7, 2.6, 3.5, 1},
        {2, 5.5, 2.4, 3.8, 1.1},
        {2, 5.5, 2.4, 3.7, 1},
        {2, 5.8, 2.7, 3.9, 1.2},
        {2, 6, 2.7, 5.1, 1.6},
        {2, 5.4, 3, 4.5, 1.5},
        {2, 6, 3.4, 4.5, 1.6},
        {2, 6.7, 3.1, 4.7, 1.5},
        {2, 6.3, 2.3, 4.4, 1.3},
        {2, 5.6, 3, 4.1, 1.3},
        {2, 5.5, 2.5, 4, 1.3},
        {2, 5.5, 2.6, 4.4, 1.2},
        {2, 6.1, 3, 4.6, 1.4},
        {2, 5.8, 2.6, 4, 1.2},
        {2, 5, 2.3, 3.3, 1},
        {2, 5.6, 2.7, 4.2, 1.3},
        {2, 5.7, 3, 4.2, 1.2},
        {2, 5.7, 2.9, 4.2, 1.3},
        {2, 6.2, 2.9, 4.3, 1.3},
        {2, 5.1, 2.5, 3, 1.1},
        {2, 5.7, 2.8, 4.1, 1.3}
    };
}
