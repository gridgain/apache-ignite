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

package org.apache.ignite.internal.processors.cache;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks that types that types that is not (yet) supported by Ignite (even though those are supported by H2) can be
 * inserted into the database and are mapped to JAVA_OBJECT.
 */
public class IgniteCacheSqlQueryUnsupportedTypeSelfTest extends GridCommonAbstractTest {
    /**
     * Starts cluster with one node.
     */
    @Before
    public void setupCluster() throws Exception {
        startGrids(1);
    }

    /**
     * Stops the cluster.
     */
    @After
    public void tearOff () {
        stopAllGrids();
    }

    /**
     * Execute sql query.
     *
     * @param sql query.
     * @param args positional args.
     * @return Fetched result of the query.
     */
    private List<List<?>> execute(String sql, Object... args) {
        return grid(0).cache("CACHE").query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args)).getAll();
    }

    private CacheConfiguration instantCacheConfiguration() {
        return new CacheConfiguration()
            .setName("CACHE")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class.getName(), Person.class.getName())
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("time", Instant.class.getName(), null)
                    .setTableName("PERSON")
                .setIndexes(Arrays.asList(
                    new QueryIndex("id", true),
                    new QueryIndex("time", true)
                ))
            ));
    }

    /**
     * Check that unsupported by IgniteSQL type is correctly inserted.
     */
    @Test
    public void testUnsupportedSqlType(){
        try (IgniteCache<Integer, Person> person = grid(0).createCache(instantCacheConfiguration())) {
            person.put(1, new Person(1));
            person.put(2, new Person(2));

            List<List<?>> res = execute("SELECT * FROM CACHE.PERSON WHERE time = (select time from cache.person where id = 1)");

            assertEquals(res.get(0).get(0), 1);
        }
    }

    /**
     * Pojo with the unsupported type.
     */
    public static class Person {
        int id;

        Instant time;

        Person(int id) {
            this.id = id;
            time = Instant.now();
        }
    }
}
