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

package org.apache.ignite.igfs;

import org.apache.commons.logging.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.igfs.common.*;
import org.apache.ignite.internal.processors.hadoop.igfs.*;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.*;

/**
 * Test interaction between a IGFS client and a IGFS server.
 */
public class IgniteHadoopFileSystemClientSelfTest extends IgfsCommonAbstractTest {
    /** Logger. */
    private static final Log LOG = LogFactory.getLog(IgniteHadoopFileSystemClientSelfTest.class);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512 * 1024);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(DFLT_IPC_PORT);

        igfsCfg.setIpcEndpointConfiguration(endpointCfg);

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    protected CacheConfiguration[] cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setEvictionPolicy(null);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new CacheConfiguration[] {metaCacheCfg, cacheCfg};
    }

    /**
     * Test output stream deferred exception (GG-4440).
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testOutputStreamDeferredException() throws Exception {
        final byte[] data = "test".getBytes();

        try {
            switchHandlerErrorFlag(true);

            HadoopIgfs client = new HadoopIgfsOutProc("127.0.0.1", 10500, getTestGridName(0), "igfs", LOG);

            client.handshake(null);

            IgfsPath path = new IgfsPath("/test1.file");

            HadoopIgfsStreamDelegate delegate = client.create(path, true, false, 1, 1024, null);

            final HadoopIgfsOutputStream igfsOut = new HadoopIgfsOutputStream(delegate, LOG,
                IgfsLogger.disabledLogger(), 0);

            // This call should return fine as exception is thrown for the first time.
            igfsOut.write(data);

            U.sleep(500);

            // This call should throw an IO exception.
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    igfsOut.write(data);

                    return null;
                }
            }, IOException.class, "Failed to write data to server (test).");
        }
        finally {
            switchHandlerErrorFlag(false);
        }
    }

    /**
     * Set IGFS REST handler error flag to the given state.
     *
     * @param flag Flag state.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void switchHandlerErrorFlag(boolean flag) throws Exception {
        IgfsProcessorAdapter igfsProc = ((IgniteKernal)grid(0)).context().igfs();

        Map<String, IgfsContext> igfsMap = getField(igfsProc, "igfsCache");

        IgfsServerManager srvMgr = F.first(igfsMap.values()).server();

        Collection<IgfsServer> srvrs = getField(srvMgr, "srvrs");

        IgfsServerHandler igfsHnd = getField(F.first(srvrs), "hnd");

        Field field = igfsHnd.getClass().getDeclaredField("errWrite");

        field.setAccessible(true);

        field.set(null, flag);
    }

    /**
     * Get value of the field with the given name of the given object.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Value of the field.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private <T> T getField(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);

        field.setAccessible(true);

        return (T)field.get(obj);
    }
}
