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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.cache.query.index.sorted.Order;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;

/**
 * Defines a signle index key.
 */
public class IndexKeyDefinition {
    /** Index key name. */
    private final String name;

    /** Index key type. {@link IndexKeyTypes}. */
    private final int idxType;

    /** Index key class. */
    private final Class<?> idxCls;

    /** Order. */
    private final Order order;

    /** */
    public IndexKeyDefinition(String name, int idxType, Class<?> idxCls, Order order) {
        this.idxType = idxType;
        this.idxCls = idxCls;
        this.order = order;
        this.name = name;
    }

    /** */
    public Order getOrder() {
        return order;
    }

    /** */
    public int getIdxType() {
        return idxType;
    }

    /** */
    public Class<?> getIdxClass() {
        return idxCls;
    }

    /** */
    public String getName() {
        return name;
    }
}
