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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Events;
    using NUnit.Framework;

    /// <summary>
    /// Tests affinity awareness functionality.
    /// </summary>
    public class AffinityAwarenessTest : ClientTestBase
    {
        // TODO:
        // * Test disabled/enabled
        // * Test hash code for all primitives
        // * Test hash code for complex key

        /** */
        private readonly List<ListLogger> _loggers = new List<ListLogger>();

        /** */
        private ICacheClient<int, int> _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityAwarenessTest"/> class.
        /// </summary>
        public AffinityAwarenessTest() : base(3)
        {
            // No-op.
        }

        /// <summary>
        /// Fixture set up.
        /// </summary>
        public override void FixtureSetUp()
        {
            base.FixtureSetUp();

            _cache = Client.CreateCache<int, int>("c");

            // Warm up client partition data.
            InitTestData();
            _cache.Get(1);
            _cache.Get(2);
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            InitTestData();
            ClearLoggers();
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void CacheGet_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.Get(key);

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void CacheGetAsync_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.GetAsync(key).Result;

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 0)]
        [TestCase(5, 0)]
        [TestCase(6, 1)]
        public void CacheGet_UserDefinedKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var cache = Client.GetOrCreateCache<TestKey, int>("c_custom_key");
            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => new TestKey(x, x.ToString()), x => x));
            cache.Get(new TestKey(1, "1")); // Warm up;

            var res = cache.Get(new TestKey(key, key.ToString()));

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        public void CachePut_UserDefinedTypeWithAffinityKey_ThrowsIgniteException()
        {
            // Note: annotation-based configuration is not supported on Java side.
            // Use manual configuration instead.
            var cacheClientConfiguration = new CacheClientConfiguration("c_custom_key_aff")
            {
                KeyConfiguration = new List<CacheKeyConfiguration>
                {
                    new CacheKeyConfiguration(typeof(TestKeyWithAffinity))
                    {
                        AffinityKeyFieldName = "_i"
                    }
                }
            };
            var cache = Client.GetOrCreateCache<TestKeyWithAffinity, int>(cacheClientConfiguration);

            var ex = Assert.Throws<IgniteException>(() => cache.Put(new TestKeyWithAffinity(1, "1"), 1));

            var expected = string.Format("Affinity keys are not supported. Object '{0}' has an affinity key.",
                typeof(TestKeyWithAffinity));

            Assert.AreEqual(expected, ex.Message);
        }

        [Test]
        public void CacheGet_NewNodeEnteredTopology_RequestIsRoutedToDefaultNode()
        {
            // Warm-up.
            Assert.AreEqual(1, _cache.Get(1));

            // Before topology change.
            Assert.AreEqual(12, _cache.Get(12));
            Assert.AreEqual(1, GetClientRequestGridIndex());

            Assert.AreEqual(14, _cache.Get(14));
            Assert.AreEqual(2, GetClientRequestGridIndex());

            // After topology change.
            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (var ignite = Ignition.Start(cfg))
            {
                // Wait for rebalance.
                var events = ignite.GetEvents();
                events.EnableLocal(EventType.CacheRebalanceStopped);
                events.WaitForLocal(EventType.CacheRebalanceStopped);

                // Warm-up.
                Assert.AreEqual(1, _cache.Get(1));

                // Assert: keys 12 and 14 belong to a new node now, but we don't have the new node in the server list.
                // Requests are routed to default node.
                Assert.AreEqual(12, _cache.Get(12));
                Assert.AreEqual(1, GetClientRequestGridIndex());

                Assert.AreEqual(14, _cache.Get(14));
                Assert.AreEqual(1, GetClientRequestGridIndex());
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void AllKeyBasedOperations_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            int unused;

            TestOperation(() => _cache.Get(key), gridIdx);
            TestAsyncOperation(() => _cache.GetAsync(key), gridIdx);

            TestOperation(() => _cache.TryGet(key, out unused), gridIdx);
            TestAsyncOperation(() => _cache.TryGetAsync(key), gridIdx);

            TestOperation(() => _cache.Put(key, key), gridIdx, "Put");
            TestAsyncOperation(() => _cache.PutAsync(key, key), gridIdx, "Put");

            TestOperation(() => _cache.PutIfAbsent(key, key), gridIdx, "PutIfAbsent");
            TestAsyncOperation(() => _cache.PutIfAbsentAsync(key, key), gridIdx, "PutIfAbsent");

            TestOperation(() => _cache.GetAndPutIfAbsent(key, key), gridIdx, "GetAndPutIfAbsent");
            TestAsyncOperation(() => _cache.GetAndPutIfAbsentAsync(key, key), gridIdx, "GetAndPutIfAbsent");

            TestOperation(() => _cache.Clear(key), gridIdx, "ClearKey");
            TestAsyncOperation(() => _cache.ClearAsync(key), gridIdx, "ClearKey");

            TestOperation(() => _cache.ContainsKey(key), gridIdx, "ContainsKey");
            TestAsyncOperation(() => _cache.ContainsKeyAsync(key), gridIdx, "ContainsKey");

            TestOperation(() => _cache.GetAndPut(key, key), gridIdx, "GetAndPut");
            TestAsyncOperation(() => _cache.GetAndPutAsync(key, key), gridIdx, "GetAndPut");

            TestOperation(() => _cache.GetAndReplace(key, key), gridIdx, "GetAndReplace");
            TestAsyncOperation(() => _cache.GetAndReplaceAsync(key, key), gridIdx, "GetAndReplace");

            TestOperation(() => _cache.GetAndRemove(key), gridIdx, "GetAndRemove");
            TestAsyncOperation(() => _cache.GetAndRemoveAsync(key), gridIdx, "GetAndRemove");

            TestOperation(() => _cache.Replace(key, key), gridIdx, "Replace");
            TestAsyncOperation(() => _cache.ReplaceAsync(key, key), gridIdx, "Replace");

            TestOperation(() => _cache.Replace(key, key, key + 1), gridIdx, "ReplaceIfEquals");
            TestAsyncOperation(() => _cache.ReplaceAsync(key, key, key + 1), gridIdx, "ReplaceIfEquals");

            TestOperation(() => _cache.Remove(key), gridIdx, "RemoveKey");
            TestAsyncOperation(() => _cache.RemoveAsync(key), gridIdx, "RemoveKey");

            TestOperation(() => _cache.Remove(key, key), gridIdx, "RemoveIfEquals");
            TestAsyncOperation(() => _cache.RemoveAsync(key, key), gridIdx, "RemoveIfEquals");
        }

        [Test]
        public void CacheGet_RepeatedCall_DoesNotRequestAffinityMapping()
        {
            // Test that affinity mapping is not requested when known.
            // Start new cache to enforce partition mapping request.
            Client.CreateCache<int, int>("repeat-call-test");
            ClearLoggers();

            _cache.Get(1);
            _cache.Get(1);
            _cache.Get(1);

            var requests = GetCacheRequestNames(_loggers[1]).ToArray();

            var expectedRequests = new[]
            {
                "Partitions",
                "Get",
                "Get",
                "Get"
            };

            Assert.AreEqual(expectedRequests, requests);
        }

        [Test]
        public void CacheGet_AffinityAwarenessDisabled_RoutesRequestsWithRoundRobin()
        {
            var cfg = GetClientConfiguration();
            cfg.EnableAffinityAwareness = false;

            using (var client = Ignition.StartClient(cfg))
            {
                var cache = client.GetCache<int, int>(_cache.Name);

                var requestTargets = Enumerable
                    .Range(1, 10)
                    .Select(x =>
                    {
                        cache.Get(x);
                        return GetClientRequestGridIndex();
                    })
                    .Distinct()
                    .ToArray();

                // Affinity awareness disabled - all requests go to same socket, picked with round-robin on connect.
                Assert.AreEqual(1, requestTargets.Length);
            }
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var cfg = base.GetIgniteConfiguration();

            var logger = new ListLogger();
            cfg.Logger = logger;
            _loggers.Add(logger);

            return cfg;
        }

        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            var cfg = base.GetClientConfiguration();

            cfg.EnableAffinityAwareness = true;
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 1));
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 2));

            return cfg;
        }

        private int GetClientRequestGridIndex(string message = null)
        {
            message = message ?? "Get";

            try
            {
                for (var i = 0; i < _loggers.Count; i++)
                {
                    var requests = GetCacheRequestNames(_loggers[i]);

                    if (requests.Contains(message))
                    {
                        return i;
                    }
                }

                return -1;
            }
            finally
            {
                ClearLoggers();
            }
        }

        private static IEnumerable<string> GetCacheRequestNames(ListLogger logger)
        {
            var messageRegex = new Regex(
                @"Client request received \[reqId=\d+, addr=/127.0.0.1:\d+, " +
                @"req=org.apache.ignite.internal.processors.platform.client.cache.ClientCache(\w+)Request@");

            return logger.Messages
                .Select(m => messageRegex.Match(m))
                .Where(m => m.Success)
                .Select(m => m.Groups[1].Value);
        }


        private void ClearLoggers()
        {
            foreach (var logger in _loggers)
            {
                logger.Clear();
            }
        }

        private void TestOperation(Action action, int expectedGridIdx, string message = null)
        {
            InitTestData();
            ClearLoggers();
            action();
            Assert.AreEqual(expectedGridIdx, GetClientRequestGridIndex(message));
        }

        private void TestAsyncOperation<T>(Func<T> action, int expectedGridIdx, string message = null)
            where T : Task
        {
            ClearLoggers();
            action().Wait();
            Assert.AreEqual(expectedGridIdx, GetClientRequestGridIndex(message));
        }

        private void InitTestData()
        {
            _cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));
        }
    }
}
