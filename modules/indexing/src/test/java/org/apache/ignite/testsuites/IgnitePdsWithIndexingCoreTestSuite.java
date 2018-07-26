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
package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheHistoricalRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxHistoricalRebalancingTest;

/**
 * Test suite for tests that cover core PDS features and depend on indexing module.
 */
public class IgnitePdsWithIndexingCoreTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Persistent Store With Indexing Test Suite");

/*
        suite.addTestSuite(IgnitePdsCacheIntegrationTest.class);
        suite.addTestSuite(IgnitePdsPageEvictionTest.class);
        suite.addTestSuite(IgnitePdsMultiNodePutGetRestartTest.class);
        suite.addTestSuite(IgnitePersistentStoreCacheGroupsTest.class);
        suite.addTestSuite(PersistenceDirectoryWarningLoggingTest.class);
        suite.addTestSuite(WalPathsTest.class);
        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);

        suite.addTestSuite(IgniteWalRecoveryTest.class);
        suite.addTestSuite(IgniteWalRecoveryWithCompactionTest.class);
        suite.addTestSuite(IgnitePdsNoActualWalHistoryTest.class);
        suite.addTestSuite(IgniteWalRebalanceTest.class);

        suite.addTestSuite(IgnitePdsAtomicCacheRebalancingTest.class);

        suite.addTestSuite(IgnitePdsTxCacheRebalancingTest.class);
*/

        suite.addTestSuite(IgnitePdsTxHistoricalRebalancingTest.class);
        suite.addTestSuite(IgnitePdsAtomicCacheHistoricalRebalancingTest.class);

/*
        suite.addTestSuite(IgniteWalRecoveryPPCTest.class);
        suite.addTestSuite(IgnitePdsDiskErrorsRecoveringTest.class);
        suite.addTestSuite(IgnitePdsCacheDestroyDuringCheckpointTest.class);

        suite.addTestSuite(IgnitePdsBinaryMetadataOnClusterRestartTest.class);
        suite.addTestSuite(IgnitePdsMarshallerMappingRestoreOnNodeStartTest.class);
        suite.addTestSuite(IgnitePdsThreadInterruptionTest.class);
        suite.addTestSuite(IgnitePdsBinarySortObjectFieldsTest.class);

        suite.addTestSuite(IgnitePdsCorruptedIndexTest.class);
*/

        return suite;
    }
}
