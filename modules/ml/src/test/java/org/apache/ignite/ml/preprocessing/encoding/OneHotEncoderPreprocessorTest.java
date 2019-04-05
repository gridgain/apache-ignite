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

package org.apache.ignite.ml.preprocessing.encoding;

import java.util.HashMap;
import java.util.HashSet;
import org.apache.ignite.ml.math.exceptions.preprocessing.UnknownCategorialFeatureValue;
import org.apache.ignite.ml.preprocessing.encoding.onehotencoder.OneHotEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderPreprocessor;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StringEncoderPreprocessor}.
 */
public class OneHotEncoderPreprocessorTest {
    /** Tests {@code apply()} method. */
    @Test
    public void testApplyWithStringValues() {
        String[][] data = new String[][]{
            {"1", "Moscow", "A"},
            {"2", "Moscow", "A"},
            {"2", "Moscow", "B"},
        };

        OneHotEncoderPreprocessor<Integer, String[]> preprocessor = new OneHotEncoderPreprocessor<Integer, String[]>(
            new HashMap[]{new HashMap() {
                {
                    put("1", 1);
                    put("2", 0);
                }
            }, new HashMap() {
                {
                    put("Moscow", 0);
                }
            }, new HashMap() {
                {
                    put("A", 0);
                    put("B", 1);
                }
            }},
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][]{
            {0.0, 1.0, 1.0, 1.0, 0.0},
            {1.0, 0.0, 1.0, 1.0, 0.0},
            {1.0, 0.0, 1.0, 0.0, 1.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);
    }


    /**  */
    @Test
    public void testOneCategorialFeature() {
        String[][] data = new String[][]{
            {"42"},
            {"43"},
            {"42"},
        };

        OneHotEncoderPreprocessor<Integer, String[]> preprocessor = new OneHotEncoderPreprocessor<Integer, String[]>(
            new HashMap[]{new HashMap() {
                {
                    put("42", 0);
                    put("43", 1);
                }
            }},
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                }
            });

        double[][] postProcessedData = new double[][]{
            {1.0, 0.0},
            {0.0, 1.0},
            {1.0, 0.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);
    }

    /**  */
    @Test
    public void testTwoCategorialFeatureAndTwoDoubleFeatures() {
        Object[][] data = new Object[][]{
            {"42", 1.0, "M", 2.0},
            {"43", 2.0, "F", 3.0},
            {"42", 3.0, Double.NaN, 4.0},
            {"42", 4.0, "F", 5.0},
        };

        HashMap[] encodingValues = new HashMap[4];
        encodingValues[0] = new HashMap() {
            {
                put("42", 0);
                put("43", 1);
            }
        };

        encodingValues[2] = new HashMap() {
            {
                put("F", 0);
                put("M", 1);
                put("", 2);
            }
        };

        OneHotEncoderPreprocessor<Integer, Object[]> preprocessor = new OneHotEncoderPreprocessor<Integer, Object[]>(
            encodingValues,
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][]{
            {1.0, 2.0, 1.0, 0.0, 0.0, 1.0, 0.0},
            {2.0, 3.0, 0.0, 1.0, 1.0, 0.0, 0.0},
            {3.0, 4.0, 1.0, 0.0, 0.0, 0.0, 1.0},
            {4.0, 5.0, 1.0, 0.0, 1.0, 0.0, 0.0},
        };

        for (int i = 0; i < data.length; i++)
            assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);
    }

    /**
     * The {@code apply()} method is failed with UnknownCategorialFeatureValue exception.
     *
     * The reason is missed information in encodingValues.
     *
     * @see UnknownCategorialFeatureValue
     */
    @Test
    public void testApplyWithUnknownGategorialValues() {
        String[][] data = new String[][]{
            {"1", "Moscow", "A"},
            {"2", "Moscow", "A"},
            {"2", "Moscow", "B"},
        };

        OneHotEncoderPreprocessor<Integer, String[]> preprocessor = new OneHotEncoderPreprocessor<Integer, String[]>(
            new HashMap[]{new HashMap() {
                {
                    put("2", 0);
                }
            }, new HashMap() {
                {
                    put("Moscow", 0);
                }
            }, new HashMap() {
                {
                    put("A", 0);
                    put("B", 1);
                }
            }},
            (k, v) -> v,
            new HashSet() {
                {
                    add(0);
                    add(1);
                    add(2);
                }
            });

        double[][] postProcessedData = new double[][]{
            {0.0, 1.0, 1.0, 1.0, 0.0},
            {1.0, 0.0, 1.0, 1.0, 0.0},
            {1.0, 0.0, 1.0, 0.0, 1.0},
        };

        try {
            for (int i = 0; i < data.length; i++)
                assertArrayEquals(postProcessedData[i], preprocessor.apply(i, data[i]).asArray(), 1e-8);

            fail("UnknownCategorialFeatureValue");
        } catch (UnknownCategorialFeatureValue e) {
            return;
        }
        fail("UnknownCategorialFeatureValue");
    }
}
