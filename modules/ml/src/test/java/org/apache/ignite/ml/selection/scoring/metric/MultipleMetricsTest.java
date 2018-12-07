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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Arrays;
import org.apache.ignite.ml.selection.scoring.TestLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Evaluator}.
 */
public class MultipleMetricsTest {
    /** */
    @Test
    public void testScore() {
        Metric<Integer> scoreCalculator = new Accuracy<>();

        LabelPairCursor<Integer> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1, 1, 1, 1),
            Arrays.asList(1, 1, 0, 1)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals(0.75, score, 1e-12);
    }

    // Test 1: duplicate metrics
    // Test 2: metrics with unexisting labels
    // Test 3: one metric for different classes (should work correctly)
}
