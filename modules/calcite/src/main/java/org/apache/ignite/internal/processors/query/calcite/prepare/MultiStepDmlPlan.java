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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed dml plan.
 */
public class MultiStepDmlPlan extends AbstractMultiStepPlan {
    /**
     * @param fragments Query fragments.
     */
    public MultiStepDmlPlan(List<Fragment> fragments) {
        this(fragments, null);
    }

    /**
     * @param fragments Query fragments.
     * @param fieldsMeta Fields metadata.
     */
    public MultiStepDmlPlan(List<Fragment> fragments, FieldsMetadata fieldsMeta) {
        this(fragments, fieldsMeta, new QueryMappings());
    }

    /** */
    private MultiStepDmlPlan(List<Fragment> fragments, FieldsMetadata fieldsMetadata, QueryMappings mappings) {
        super(fragments, fieldsMetadata, mappings);
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.DML;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public QueryPlan clone(@NotNull PlanningContext ctx) {
        return new MultiStepDmlPlan(new Cloner(ctx.cluster()).go(fragments), fieldsMetadata, queryMappings);
    }
}
