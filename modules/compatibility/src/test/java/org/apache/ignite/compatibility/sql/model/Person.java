/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.model;

import java.util.Collections;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import static org.apache.ignite.compatibility.sql.model.City.Factory.CITY_CNT;
import static org.apache.ignite.compatibility.sql.model.ModelUtil.randomString;

/**
 * Person entity.
 */
public class Person {
    /**  */
    @QuerySqlField
    private final String name;

    /** */
    @QuerySqlField
    private final String depCode;

    /**  */
    @QuerySqlField
    private final int age;

    /**  */
    @QuerySqlField
    private final int cityId;

    /**  */
    @QuerySqlField
    private final String position;

    /**  */
    public Person(String name, String depCode, int age, int cityId, String position) {
        this.name = name;
        this.depCode = depCode;
        this.age = age;
        this.cityId = cityId;
        this.position = position;
    }

    /**  */
    public String Name() {
        return name;
    }

    /**  */
    public String DepCode() {
        return depCode;
    }

    /**  */
    public int Age() {
        return age;
    }

    /**  */
    public int CityId() {
        return cityId;
    }

    /**  */
    public String Position() {
        return position;
    }

    /**  */
    public static class Factory implements ModelFactory {
        /** Table name. */
        private static final String TABLE_NAME = "person";

        /** Person count. */
        private static final int PERSON_CNT = 10_000; // TODO scale

        /**  */
        private final Random rnd;

        /**  */
        private final QueryEntity qryEntity;

        /**  */
        public Factory(int seed) {
            this.rnd = new Random(seed);
            QueryEntity entity = new QueryEntity(Long.class, Person.class);
            entity.setKeyFieldName("id");
            entity.addQueryField("id", Long.class.getName(), null);
            Set<QueryIndex> indices = Collections.singleton(new QueryIndex("depCode", QueryIndexType.SORTED));

            entity.setIndexes(indices);
            entity.setTableName(TABLE_NAME);
            this.qryEntity = entity;
        }

        /** {@inheritDoc} */
        @Override public Person createRandom() {
            return new Person(
                randomString(rnd, 1, 10), // name
                randomString(rnd, 1, 1), // depCode == 1 char
                rnd.nextInt(60), // age
                rnd.nextInt(CITY_CNT), // cityId
                randomString(rnd, 0, 10) // position
            );
        }

        /** {@inheritDoc} */
        @Override public QueryEntity queryEntity() {
            return qryEntity;
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return TABLE_NAME;
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return PERSON_CNT;
        }
    }
}
