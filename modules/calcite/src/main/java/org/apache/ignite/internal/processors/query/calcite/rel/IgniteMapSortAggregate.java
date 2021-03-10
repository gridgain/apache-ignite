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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;

/**
 *
 */
public class IgniteMapSortAggregate extends IgniteMapAggregateBase {
    /** Collation. */
    private final RelCollation collation;

    /** */
    public IgniteMapSortAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        RelCollation collation
    ) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);

        assert Objects.nonNull(collation);
        assert !collation.isDefault();

        this.collation = collation;
    }

    /** */
    public IgniteMapSortAggregate(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        collation = input.getCollation();

        assert Objects.nonNull(collation);
        assert !collation.isDefault();
    }

    /** {@inheritDoc} */
    @Override public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
        return new IgniteMapSortAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, collation);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMapSortAggregate(
            cluster,
            getTraitSet(),
            sole(inputs),
            getGroupSet(),
            getGroupSets(),
            getAggCallList(),
            collation
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = Commons.typeFactory(getCluster());

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        groupSet.forEach(fieldIdx -> {
            RelDataTypeField fld = input.getRowType().getFieldList().get(fieldIdx);

            builder.add(fld);
        });

        builder.add("AGG_DATA", typeFactory.createArrayType(typeFactory.createJavaType(Accumulator.class), -1));

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return computeSelfCostSort(planner, mq);
    }

    /** {@inheritDoc} */
    @Override public RelCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
        RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(groupSet.asList()));

        return Pair.of(nodeTraits.replace(collation),
            ImmutableList.of(inputTraits.get(0).replace(collation)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation inputCollation = TraitUtils.collation(inputTraits.get(0));

        List<Integer> newCollation = maxPrefix(inputCollation.getKeys(), groupSet.asSet());

        if (newCollation.size() < groupSet.cardinality())
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(
            nodeTraits.replace(RelCollations.of(ImmutableIntList.copyOf(newCollation))),
            inputTraits
        ));
    }
}
