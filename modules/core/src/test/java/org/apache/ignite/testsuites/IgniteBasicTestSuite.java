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
import org.apache.ignite.GridSuppressedExceptionSelfTest;
import org.apache.ignite.failure.FailureHandlerTriggeredTest;
import org.apache.ignite.failure.OomFailureHandlerTest;
import org.apache.ignite.failure.StopNodeFailureHandlerTest;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandlerTest;
import org.apache.ignite.internal.ClassSetTest;
import org.apache.ignite.internal.ClusterGroupHostsSelfTest;
import org.apache.ignite.internal.ClusterGroupSelfTest;
import org.apache.ignite.internal.GridFailFastNodeFailureDetectionSelfTest;
import org.apache.ignite.internal.GridLifecycleAwareSelfTest;
import org.apache.ignite.internal.GridLifecycleBeanSelfTest;
import org.apache.ignite.internal.GridMBeansTest;
import org.apache.ignite.internal.GridNodeMetricsLogSelfTest;
import org.apache.ignite.internal.GridProjectionForCachesSelfTest;
import org.apache.ignite.internal.GridReduceSelfTest;
import org.apache.ignite.internal.GridReleaseTypeSelfTest;
import org.apache.ignite.internal.GridSelfTest;
import org.apache.ignite.internal.GridStartStopSelfTest;
import org.apache.ignite.internal.GridStopWithCancelSelfTest;
import org.apache.ignite.internal.IgniteLocalNodeMapBeforeStartTest;
import org.apache.ignite.internal.IgniteSlowClientDetectionSelfTest;
import org.apache.ignite.internal.MarshallerContextLockingSelfTest;
import org.apache.ignite.internal.TransactionsMXBeanImplTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.processors.DeadLockOnNodeLeftExchangeTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2Test;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2TestNoOptimizations;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorMemoryLeakTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorRendezvousSelfTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTestNoOptimization;
import org.apache.ignite.internal.processors.cache.GridLocalIgniteSerializationTest;
import org.apache.ignite.internal.processors.cache.GridProjectionForCachesOnDaemonNodeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDaemonNodeMarshallerCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClassNameConflictTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingOnMissTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheConcurrentReadWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheFSRestoreTest;
import org.apache.ignite.internal.processors.cache.RebalanceWithDifferentThreadPoolSizeTest;
import org.apache.ignite.internal.processors.cache.SetTxTimeoutOnPartitionMapExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteRejectConnectOnNodeStopTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithSystemWorkerDeathTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorRemoteTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorSelfTest;
import org.apache.ignite.internal.processors.closure.GridClosureSerializationTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.internal.processors.continuous.GridMessageListenSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeFakeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeSelfTest;
import org.apache.ignite.internal.processors.database.CacheFreeListImplSelfTest;
import org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest;
import org.apache.ignite.internal.processors.database.IndexStorageSelfTest;
import org.apache.ignite.internal.processors.database.SwapPathConstructionSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcEscapeSequenceSelfTest;
import org.apache.ignite.internal.processors.service.ClosureServiceClientsNodesTest;
import org.apache.ignite.internal.product.GridProductVersionSelfTest;
import org.apache.ignite.internal.util.BitSetIntSetTest;
import org.apache.ignite.internal.util.GridCleanerTest;
import org.apache.ignite.internal.util.nio.IgniteExceptionInNioWorkerSelfTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.MarshallerContextSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.messaging.IgniteMessagingSendAsyncTest;
import org.apache.ignite.messaging.IgniteMessagingWithClientTest;
import org.apache.ignite.plugin.PluginNodeValidationTest;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilderTest;
import org.apache.ignite.spi.GridSpiLocalHostInjectionTest;
import org.apache.ignite.startup.properties.NotStringSystemPropertyTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.test.ConfigVariationsTestSuiteBuilderTest;
import org.apache.ignite.testframework.test.ListeningTestLoggerTest;
import org.apache.ignite.testframework.test.ParametersTest;
import org.apache.ignite.testframework.test.VariationsIteratorTest;
import org.apache.ignite.util.AttributeNodeFilterSelfTest;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteMarshallerSelfTestSuite.class,
    IgniteLangSelfTestSuite.class,
    IgniteUtilSelfTestSuite.class,

    IgniteKernalSelfTestSuite.class,
    IgniteStartUpTestSuite.class,
    IgniteExternalizableSelfTestSuite.class,
    IgniteP2PSelfTestSuite.class,
    IgniteCacheP2pUnmarshallingErrorTestSuite.class,
    IgniteStreamSelfTestSuite.class,

    IgnitePlatformsTestSuite.class,

    GridSelfTest.class,
    ClusterGroupHostsSelfTest.class,
    IgniteMessagingWithClientTest.class,
    IgniteMessagingSendAsyncTest.class,

    ClusterGroupSelfTest.class,
    GridMessagingSelfTest.class,
    GridMessagingNoPeerClassLoadingSelfTest.class,

    GridReleaseTypeSelfTest.class,
    GridProductVersionSelfTest.class,
    GridAffinityAssignmentV2Test.class,
    GridAffinityAssignmentV2TestNoOptimizations.class,
    GridHistoryAffinityAssignmentTest.class,
    GridHistoryAffinityAssignmentTestNoOptimization.class,
    GridAffinityProcessorRendezvousSelfTest.class,
    GridAffinityProcessorMemoryLeakTest.class,
    GridClosureProcessorSelfTest.class,
    GridClosureProcessorRemoteTest.class,
    GridClosureSerializationTest.class,
    GridStartStopSelfTest.class,
    GridProjectionForCachesSelfTest.class,
    GridProjectionForCachesOnDaemonNodeSelfTest.class,
    GridSpiLocalHostInjectionTest.class,
    GridLifecycleBeanSelfTest.class,
    GridStopWithCancelSelfTest.class,
    GridReduceSelfTest.class,
    GridEventConsumeSelfTest.class,
    GridSuppressedExceptionSelfTest.class,
    GridLifecycleAwareSelfTest.class,
    GridMessageListenSelfTest.class,
    GridFailFastNodeFailureDetectionSelfTest.class,
    IgniteSlowClientDetectionSelfTest.class,
    IgniteDaemonNodeMarshallerCacheTest.class,
    IgniteMarshallerCacheConcurrentReadWriteTest.class,
    GridNodeMetricsLogSelfTest.class,
    GridLocalIgniteSerializationTest.class,
    GridMBeansTest.class,
    TransactionsMXBeanImplTest.class,
    SetTxTimeoutOnPartitionMapExchangeTest.class,

    IgniteExceptionInNioWorkerSelfTest.class,
    IgniteLocalNodeMapBeforeStartTest.class,
    OdbcConfigurationValidationSelfTest.class,
    OdbcEscapeSequenceSelfTest.class,

    DynamicProxySerializationMultiJvmSelfTest.class,

    MarshallerContextLockingSelfTest.class,
    MarshallerContextSelfTest.class,

    SecurityPermissionSetBuilderTest.class,

    AttributeNodeFilterSelfTest.class,

    // Basic DB data structures.
    BPlusTreeSelfTest.class,
    BPlusTreeFakeReuseSelfTest.class,
    BPlusTreeReuseSelfTest.class,
    IndexStorageSelfTest.class,
    CacheFreeListImplSelfTest.class,
    DataRegionMetricsSelfTest.class,
    SwapPathConstructionSelfTest.class,
    BitSetIntSetTest.class,

    IgniteMarshallerCacheFSRestoreTest.class,
    IgniteMarshallerCacheClassNameConflictTest.class,
    IgniteMarshallerCacheClientRequestsMappingOnMissTest.class,

    IgniteDiagnosticMessagesTest.class,
    IgniteDiagnosticMessagesMultipleConnectionsTest.class,

    IgniteRejectConnectOnNodeStopTest.class,

    GridCleanerTest.class,

    ClassSetTest.class,

    // Basic failure handlers.
    FailureHandlerTriggeredTest.class,
    StopNodeFailureHandlerTest.class,
    StopNodeOrHaltFailureHandlerTest.class,
    OomFailureHandlerTest.class,
    TransactionIntegrityWithSystemWorkerDeathTest.class,

    AtomicOperationsInTxTest.class,

    CacheRebalanceConfigValidationTest.class,

    ListeningTestLoggerTest.class,

    CacheLocalGetSerializationTest.class,

    PluginNodeValidationTest.class,

    // In-memory Distributed MetaStorage.
    DistributedMetaStorageTest.class,
    DistributedConfigurationInMemoryTest.class,

    ConsistentIdImplicitlyExplicitlyTest.class,

        // Tests against configuration variations framework.
        suite.addTestSuite(ParametersTest.class);
        suite.addTestSuite(VariationsIteratorTest.class);
        suite.addTestSuite(ConfigVariationsTestSuiteBuilderTest.class);
        suite.addTestSuite(NotStringSystemPropertyTest.class);

        suite.addTestSuite(MarshallerContextLockingSelfTest.class);
        suite.addTestSuite(MarshallerContextSelfTest.class);

        suite.addTestSuite(SecurityPermissionSetBuilderTest.class);

        suite.addTestSuite(AttributeNodeFilterSelfTest.class);

        // Basic DB data structures.
        suite.addTestSuite(BPlusTreeSelfTest.class);
        suite.addTestSuite(BPlusTreeFakeReuseSelfTest.class);
        suite.addTestSuite(BPlusTreeReuseSelfTest.class);
        suite.addTestSuite(IndexStorageSelfTest.class);
        suite.addTestSuite(CacheFreeListImplSelfTest.class);
        suite.addTestSuite(DataRegionMetricsSelfTest.class);
        suite.addTestSuite(SwapPathConstructionSelfTest.class);
        suite.addTestSuite(BitSetIntSetTest.class);

        suite.addTestSuite(IgniteMarshallerCacheFSRestoreTest.class);
        suite.addTestSuite(IgniteMarshallerCacheClassNameConflictTest.class);
        suite.addTestSuite(IgniteMarshallerCacheClientRequestsMappingOnMissTest.class);

        suite.addTestSuite(IgniteDiagnosticMessagesTest.class);

        suite.addTestSuite(IgniteRejectConnectOnNodeStopTest.class);

        suite.addTestSuite(GridCleanerTest.class);

        suite.addTestSuite(ClassSetTest.class);

        // Basic failure handlers.
        suite.addTestSuite(FailureHandlerTriggeredTest.class);
        suite.addTestSuite(StopNodeFailureHandlerTest.class);
        suite.addTestSuite(StopNodeOrHaltFailureHandlerTest.class);
        suite.addTestSuite(OomFailureHandlerTest.class);
        suite.addTestSuite(TransactionIntegrityWithSystemWorkerDeathTest.class);

        suite.addTestSuite(RebalanceWithDifferentThreadPoolSizeTest.class);

        suite.addTestSuite(ListeningTestLoggerTest.class);

        suite.addTestSuite(DeadLockOnNodeLeftExchangeTest.class);

        return suite;
    }
}
