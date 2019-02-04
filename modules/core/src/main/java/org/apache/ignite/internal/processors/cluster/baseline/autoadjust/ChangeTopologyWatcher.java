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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;

/**
 * Watcher of topology changes. It initiate to set new baseline after some timeout.
 */
public class ChangeTopologyWatcher implements GridLocalEventListener {
    /** Task represented NULL value is using when normal task can not be created. */
    private static final BaselineAutoAdjustData NULL_BASELINE_DATA = new BaselineAutoAdjustData(null, -1);
    /** */
    private final IgniteLogger log;
    /** */
    private final IgniteClusterImpl cluster;
    /** */
    private final GridCachePartitionExchangeManager<?, ?> exchangeManager;
    /** Configuration of baseline. */
    private final DistributedBaselineConfiguration baselineConfiguration;
    /** Discovery manager. */
    private final GridDiscoveryManager discoveryMgr;
    /** */
    private final GridClusterStateProcessor stateProcessor;
    /** Scheduler of specific task of baseline changing. */
    private final BaselineAutoAdjustScheduler baselineAutoAdjustScheduler;

    /** Last data for set new baseline. */
    private BaselineAutoAdjustData lastBaselineData = NULL_BASELINE_DATA;

    /**
     * @param ctx Context.
     */
    public ChangeTopologyWatcher(GridKernalContext ctx) {
        this.log = ctx.log(ChangeTopologyWatcher.class);
        this.cluster = ctx.cluster().get();
        this.baselineConfiguration = cluster.baselineConfiguration();
        this.exchangeManager = ctx.cache().context().exchange();
        this.stateProcessor = ctx.state();
        this.baselineAutoAdjustScheduler = new BaselineAutoAdjustScheduler(ctx.timeout(), new BaselineAutoAdjustExecutor(
            ctx.log(BaselineAutoAdjustExecutor.class),
            cluster,
            ctx.getSystemExecutorService(),
            this::isBaselineAutoAdjustEnabled
        ));
        this.discoveryMgr = ctx.discovery();
    }

    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        if (!isBaselineAutoAdjustEnabled()) {
            synchronized (this) {
                lastBaselineData = NULL_BASELINE_DATA;
            }

            return;
        }

        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

        if (discoEvt.eventNode().isClient() || discoEvt.eventNode().isDaemon())
            return;

        synchronized (this) {
            lastBaselineData = lastBaselineData.next(evt, discoEvt.topologyVersion());

            if (isLocalNodeCoordinator(discoveryMgr)) {
                exchangeManager.affinityReadyFuture(new AffinityTopologyVersion(discoEvt.topologyVersion()))
                    .listen((IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>)future -> {

                        if (exchangeManager.lastFinishedFuture().hasLostPartitions()) {
                            disableBaselineAutoAdjust("lost partitions was detected");

                            return;
                        }

                        long timeout = baselineConfiguration.getBaselineAutoAdjustTimeout();

                        log.info("Baseline will be changed in '" + timeout + "' ms - ");

                        baselineAutoAdjustScheduler.schedule(lastBaselineData, timeout);
                    });

            }
        }
    }

    /**
     * Disable baseline auto adjust property in cluster.
     *
     * @param reason Reason of disable.
     */
    private void disableBaselineAutoAdjust(String reason) {
        log.warning("Baseline auto-adjust will be disable due to " + reason);

        try {
            baselineConfiguration.updateBaselineAutoAdjustEnabledAsync(false);
        }
        catch (IgniteCheckedException e) {
            log.error("Error during disable baseline auto-adjust", e);
        }
    }

    /**
     * @return {@code true} if auto-adjust baseline enabled.
     */
    private boolean isBaselineAutoAdjustEnabled() {
        return stateProcessor.clusterState().active() && baselineConfiguration.isBaselineAutoAdjustEnabled();
    }
}
