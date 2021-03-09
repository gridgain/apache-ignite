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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JoinCollocationIntegrationTest extends AbstractBasicIntegrationTest {
    private IgniteEx client;

    @Before
    public void init() {
        client = grid("client");
    }

    @Test
    public void joinOfSameTablesShouldBeCollocatedSimpleKey() {
        execQuery("drop table if exists MyTable");
        execQuery("create table MyTable (id int primary key, val int)");
        // TODO: IGNITE-14176 currently PK index is not considered for planning,
        // thus we have to create it ourselves
        execQuery("create index MyTable_pk on MyTable(id)");

        for (int i = 0; i < 500; i++)
            execQuery("insert into MyTable values (?, ?)", i, i);

        assertQuery("select * from MyTable t1 join MyTable t2 on t1.id = t2.id")
            .returns(100L)
            .check();
    }

    /** */
    private List<List<?>> execQuery(String sql, Object... args) {
        return client.context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
