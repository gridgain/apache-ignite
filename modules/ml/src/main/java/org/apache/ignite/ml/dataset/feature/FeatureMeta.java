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

package org.apache.ignite.ml.dataset.feature;

/**
 * Feature meta class.
 */
public class FeatureMeta {
    /** Id of feature in feature vector. */
    private final int featureId;

    /** Is categorical feature flag. */
    private final boolean isCategoricalFeature;

    /**
     * Create an instance of Feature meta.
     *
     * @param featureId Feature id.
     * @param isCategoricalFeature Is categorical feature.
     */
    public FeatureMeta(int featureId, boolean isCategoricalFeature) {
        this.featureId = featureId;
        this.isCategoricalFeature = isCategoricalFeature;
    }

    /** */
    public int getFeatureId() {
        return featureId;
    }

    /** */
    public boolean isCategoricalFeature() {
        return isCategoricalFeature;
    }
}
