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

package org.apache.ignite.internal;

import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;

/**
 * Tests version methods.
 */
public class GridVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_UPDATE_NOTIFIER, value = "true")
    public void testVersions() throws Exception {
        try {
            final IgniteEx ignite = (IgniteEx)clusterManager__startGrid();

            IgniteProductVersion currVer = ignite.version();

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.latestVersion() != null;
                }
            }, 2 * 60_000);

            String newVer = ignite.latestVersion();

            info("Versions [cur=" + currVer + ", latest=" + newVer + ']');

            assertNotNull(newVer);
            assertNotSame(currVer.toString(), newVer);
        }
        finally {
            stopGrid();
        }
    }
}
