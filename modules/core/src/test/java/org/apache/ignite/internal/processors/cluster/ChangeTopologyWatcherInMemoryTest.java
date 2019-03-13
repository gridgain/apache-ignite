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

package org.apache.ignite.internal.processors.cluster;

import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** */
public class ChangeTopologyWatcherInMemoryTest extends ChangeTopologyWatcherTest {
    /** {@inheritDoc} */
    @Override protected boolean isPersistent() {
        return false;
    }

    /** */
    @Test
    public void testWithZeroTimeout() throws Exception {
        startGrids(3);

        startGrid(getConfiguration(UUID.randomUUID().toString()).setClientMode(true));

        stopGrid(2);

        assertEquals(2, grid(0).cluster().currentBaselineTopology().size());

        startGrid(3);

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        startGrid(4);

        assertEquals(4, grid(0).cluster().currentBaselineTopology().size());

        stopGrid(1);

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        IgniteEx client = startGrid(getConfiguration(UUID.randomUUID().toString()).setClientMode(true));

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        stopGrid(client.name());

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());
    }
}