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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.spi.systemview.view.SqlIndexView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link SqlIndexView} attributes walker.
 * 
 * @see SqlIndexView
 */
public class SqlIndexViewWalker implements SystemViewRowAttributeWalker<SqlIndexView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheGroupId", int.class);
        v.accept(1, "cacheGroupName", String.class);
        v.accept(2, "cacheId", int.class);
        v.accept(3, "cacheName", String.class);
        v.accept(4, "schemaName", String.class);
        v.accept(5, "tableName", String.class);
        v.accept(6, "indexName", String.class);
        v.accept(7, "indexType", String.class);
        v.accept(8, "columns", String.class);
        v.accept(9, "isPk", boolean.class);
        v.accept(10, "isUnique", boolean.class);
        v.accept(11, "inlineSize", Integer.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(SqlIndexView row, AttributeWithValueVisitor v) {
        v.acceptInt(0, "cacheGroupId", row.cacheGroupId());
        v.accept(1, "cacheGroupName", String.class, row.cacheGroupName());
        v.acceptInt(2, "cacheId", row.cacheId());
        v.accept(3, "cacheName", String.class, row.cacheName());
        v.accept(4, "schemaName", String.class, row.schemaName());
        v.accept(5, "tableName", String.class, row.tableName());
        v.accept(6, "indexName", String.class, row.indexName());
        v.accept(7, "indexType", String.class, row.indexType());
        v.accept(8, "columns", String.class, row.columns());
        v.acceptBoolean(9, "isPk", row.isPk());
        v.acceptBoolean(10, "isUnique", row.isUnique());
        v.accept(11, "inlineSize", Integer.class, row.inlineSize());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 12;
    }
}
