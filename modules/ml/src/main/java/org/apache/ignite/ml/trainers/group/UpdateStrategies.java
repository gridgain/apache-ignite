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

package org.apache.ignite.ml.trainers.group;

import org.apache.ignite.ml.optimization.SmoothParametrized;
import org.apache.ignite.ml.optimization.updatecalculators.RMSPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RMSPropUpdateCalculator;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;

/**
 * Holder class for various update strategies.
 */
public class UpdateStrategies {
    /**
     * RMSProp update strategy.
     *
     * @param <M> Model for optimization.
     * @return RMSProp update strategy.
     */
    public static <M extends SmoothParametrized<M>> UpdatesStrategy<M, RMSPropParameterUpdate> RMSProp() {
        return new UpdatesStrategy<M, RMSPropParameterUpdate>(new RMSPropUpdateCalculator<>(), RMSPropParameterUpdate::sumLocal, RMSPropParameterUpdate::avg);
    }

    /**
     * RProp update strategy.
     *
     * @param <M> Model for optimization.
     * @return RProp update strategy.
     */
    public static <M extends SmoothParametrized<M>> UpdatesStrategy<M, RPropParameterUpdate> RProp() {
        return new UpdatesStrategy<>(new RPropUpdateCalculator<>(), RPropParameterUpdate::sumLocal, RPropParameterUpdate::avg);
    }
}
