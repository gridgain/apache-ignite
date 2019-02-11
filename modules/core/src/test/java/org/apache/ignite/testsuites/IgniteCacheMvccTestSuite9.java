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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStatePutTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateTwoPrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateUpdatesOrderTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateWithFilterTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({})
//@RunWith(IgniteCacheMvccTestSuite9.DynamicSuite.class)
public class IgniteCacheMvccTestSuite9 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(IgniteTxConcurrentRemoveObjectsTest.class);

        // Non supported modes.
        ignoredTests.add(IgniteTxCachePrimarySyncTest.class);

        // Atomic caches.
        ignoredTests.add(CacheAtomicPrimarySyncBackPressureTest.class);

        // Other non-tx tests.
        ignoredTests.add(IgniteCacheGetCustomCollectionsSelfTest.class);
        ignoredTests.add(IgniteCacheLoadRebalanceEvictionSelfTest.class);

        // Non-mvcc counters and history rebalance.
        ignoredTests.add(TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryOneBackupTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsTest.class);
        ignoredTests.add(TxPartitionCounterStatePutTest.class);
        ignoredTests.add(TxPartitionCounterStateTwoPrimaryTwoBackupsTest.class);
        ignoredTests.add(TxPartitionCounterStateWithFilterTest.class);
        ignoredTests.add(TxPartitionCounterStateUpdatesOrderTest.class);

        return new ArrayList<>(IgniteCacheTestSuite9.suite(ignoredTests));
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
