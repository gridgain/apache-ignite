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
import java.util.UUID;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.CollocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodeMappingException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;

/**
 * Fragment of distributed query
 */
public class Fragment {
    /** */
    private final long id;

    /** */
    private final IgniteRel root;

    /** Serialized root representation. */
    private final String rootSer;

    /** */
    private final FragmentMapping mapping;

    /** */
    private final ImmutableList<IgniteReceiver> remotes;

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes) {
        this(id, root, remotes, null, null);
    }

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     * @param rootSer Root serialized representation.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes, @Nullable String rootSer) {
        this(id, root, remotes, rootSer, null);
    }

    /** */
    Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes, @Nullable String rootSer, @Nullable FragmentMapping mapping) {
        this.id = id;
        this.root = root;
        this.remotes = ImmutableList.copyOf(remotes);
        this.rootSer = rootSer != null ? rootSer : toJson(root);
        this.mapping = mapping;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * @return Root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * Lazy serialized root representation.
     *
     * @return Serialized form.
     */
    public String serialized() {
        return rootSer;
    }

    /** */
    public FragmentMapping mapping() {
        return mapping;
    }

    /**
     * @return Fragment remote sources.
     */
    public List<IgniteReceiver> remotes() {
        return remotes;
    }

    /** */
    public boolean rootFragment() {
        return !(root instanceof IgniteSender);
    }

    /** */
    public Fragment attach(PlanningContext ctx) {
        RelOptCluster cluster = ctx.cluster();

        return root.getCluster() == cluster ? this : new Cloner(cluster).go(this);
    }

    /** */
    public Fragment detach() {
        RelOptCluster cluster = PlanningContext.empty().cluster();

        return root.getCluster() == cluster ? this : new Cloner(cluster).go(this);
    }

    /**
     * Mapps the fragment to its data location.
     * @param ctx Planner context.
     * @param mq Metadata query.
     */
    Fragment map(MappingService mappingSrvc, PlanningContext ctx, RelMetadataQuery mq) throws FragmentMappingException {
        assert root.getCluster() == ctx.cluster() : "Fragment is detached [fragment=" + this + "]";

        if (mapping != null)
            return this;

        return new Fragment(id, root, remotes, rootSer, mapping(ctx, mq, nodesSource(mappingSrvc, ctx)));
    }

    /** */
    private FragmentMapping mapping(PlanningContext ctx, RelMetadataQuery mq, Supplier<List<UUID>> nodesSource) {
        try {
            FragmentMapping mapping = IgniteMdFragmentMapping._fragmentMapping(root, mq);

            if (rootFragment())
                mapping = FragmentMapping.create(ctx.localNodeId()).colocate(mapping);

            return mapping.finalize(nodesSource);
        }
        catch (NodeMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, e.node(), e);
        }
        catch (CollocationMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, root, e);
        }
    }

    /** */
    @NotNull private Supplier<List<UUID>> nodesSource(MappingService mappingSrvc, PlanningContext ctx) {
        return () -> mappingSrvc.executionNodes(ctx.topologyVersion(), single(), null);
    }

    /** */
    private boolean single() {
        return root instanceof IgniteSender
            && ((IgniteSender)root).sourceDistribution().satisfies(IgniteDistributions.single());
    }
}
