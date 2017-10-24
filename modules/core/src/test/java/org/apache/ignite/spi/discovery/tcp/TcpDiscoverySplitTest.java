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
package org.apache.ignite.spi.discovery.tcp;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;

import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;

/**
 * {@link TcpDiscoverySpi} test with splitting
 */
public class TcpDiscoverySplitTest extends IgniteCacheTopologySplitAbstractTest {

    /** */
    private static final int SEG_0_SIZE = 4;

    /** */
    private static final long DISCO_TIMEOUT = 1000L;

    /** */
    private static final long SPLIT_TIME = 2 * DISCO_TIMEOUT + DISCO_TIMEOUT / 2;

    /** */
    private static final String NODE_IDX_ATTR = "nodeIdx";

    /** */
    private static int getDiscoPort(int gridIdx) {
        return DFLT_PORT + gridIdx;
    }

    /** */
    private static boolean isDiscoPort(int port) {
        return port >= DFLT_PORT && port <= (DFLT_PORT + TcpDiscoverySpi.DFLT_PORT_RANGE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        SplitTcpDiscoverySpi disco = (SplitTcpDiscoverySpi)cfg.getDiscoverySpi();

        disco.setSocketTimeout(DISCO_TIMEOUT);

        int idx = getTestIgniteInstanceIndex(gridName);

        cfg.setUserAttributes(Collections.singletonMap(NODE_IDX_ATTR, idx));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return isDiscoPort(locPort) && isDiscoPort(rmtPort) && segment(locPort) != segment(rmtPort);
    }

    /**  */
    private int segment(int discoPort) {
        return (discoPort - DFLT_PORT) < SEG_0_SIZE ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override protected int segment(ClusterNode node) {
        return ((Integer)node.attribute(NODE_IDX_ATTR)) < SEG_0_SIZE ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @SuppressWarnings("unchecked")
    protected void testSplitRestore(int[] startSeq, long splitTime) throws Exception {
        IgniteEx[] grids = new IgniteEx[startSeq.length];

        for (int i = 0; i < startSeq.length; i++) {
            int idx = startSeq[i];

            grids[i] = startGrid(idx);

            awaitPartitionMapExchange();
        }

        split();

        Thread.sleep(splitTime);

        unsplit(false);

        Thread.sleep(DISCO_TIMEOUT * startSeq.length - splitTime + DISCO_TIMEOUT);

        Set[] segs = {new HashSet(), new HashSet()};

        for (int i = 0; i < startSeq.length; i++) {
            int idx = startSeq[i];

            int segIdx = idx < SEG_0_SIZE ? 0 : 1;

            try {
                IgniteEx g = grids[i];

                if (!g.context().isStopping())
                    segs[segIdx].add(idx);
            }
            catch (Exception e) {
                log.warning("Error checking grid is live [idx=" + idx + ']', e);
            }
        }
        if (log.isInfoEnabled())
            for (int i = 0; i < segs.length; ++i) {
                Set seg = segs[i];

                log.info(seg.isEmpty() ? "No live grids [segment=" + i + ']' :
                    "Live grids [segment=" + i + ", size=" + seg.size() + ", indices=" + seg + ']');
            }
        int[] liveExp = startSeq;

        for (int idx : liveExp) {
            int segIdx = idx < SEG_0_SIZE ? 0 : 1;

            if (!segs[segIdx].contains(idx))
                fail("Grid is stopped, but expected to live [idx=" + idx + ']');
        }
    }

    /** */
    public void testFullSplit() throws Exception {
        int[] grids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

        testSplitRestore(grids, (grids.length - SEG_0_SIZE) * DISCO_TIMEOUT + DISCO_TIMEOUT / 2);
    }

    /** */
    public void testConsecutiveCoordSeg0() throws Exception {
        testSplitRestore(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, SPLIT_TIME);
    }

    /** */
    public void testConsecutiveCoordSeg1() throws Exception {
        testSplitRestore(new int[] {4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2, 3}, SPLIT_TIME);
    }

    /** */
    public void testShuffledCoordSeg0() throws Exception {
        testSplitRestore(new int[] {0, 4, 5, 1, 6, 7, 2, 8, 9, 3, 10, 11}, SPLIT_TIME);
    }

    /** */
    public void testShuffledCoordSeg1() throws Exception {
        testSplitRestore(new int[] {4, 5, 0, 6, 7, 1, 8, 9, 2, 10, 11, 3}, SPLIT_TIME);
    }
}
