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

package org.apache.ignite.examples.ml.util.benchmark.thinclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.BenchParameters;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class ServerMock {
    public static final String CACHE_NAME = "THIN_CLIENT_IMITATION_CACHE";

    public static void main(String... args) throws Exception {
        BenchParameters params = BenchParameters.parseArguments(args);
        System.out.println("Start servers with such configuration: [" + params.toString() + "]");

        List<Ignite> ignites = null;

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            CacheConfiguration<Integer, byte[]> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, params.getCountOfPartitions()));
            cacheConfiguration.setName(CACHE_NAME);
            cacheConfiguration.setQueryParallelism(params.getQueryParallelism());

            IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(cacheConfiguration);

            for (int i = 0; i < params.getCountOfRows(); i++) {
                byte[] val = new byte[params.getValueObjectSizeInBytes()];
                Arrays.fill(val, (byte)i);
                cache.put(i, val);
            }

            ignites = startIgnites(params);

            System.out.println("Cache is ready! [rows = " + params.getCountOfRows() + "]");

            Thread.currentThread().join();
        }
        finally {
            if (ignites != null) {
                for (Ignite ign : ignites)
                    ign.close();
            }
        }
    }

    private static List<Ignite> startIgnites(BenchParameters params) throws IgniteCheckedException {
        List<Ignite> nodes = new ArrayList<>();

        for (int idx = 0; idx < params.getCountOfIgnites() - 1; idx++) {
            IgniteConfiguration configuration = new IgniteConfiguration();
            configuration
                .setIgniteInstanceName("node_" + idx)
                .setPeerClassLoadingEnabled(true)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList("127.0.0.1:47500..47509"))))
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setHost("127.0.0.1")
                    .setPort(10800 + idx)
                    .setPortRange(1));

            nodes.add(IgnitionEx.start(configuration));
        }

        return nodes;
    }
}
