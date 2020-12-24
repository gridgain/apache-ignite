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

/**
 * Defines IndexKey.
 */
public class IndexKeyDefinition {
    /** Index key name. */
    private final String name;

    /** Index key type. */
    private final int idxType;

    /** Order. */
    private final Order order;

    /** Constructor. */
    public IndexKeyDefinition(String name, int idxType, Order order) {
        this.idxType = idxType;
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
    public String getName() {
        return name;
    }
}
