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
 *
 */

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlParserUtils;

/**
 * KILL QUERY command.
 */
public class SqlKillQueryCommand implements SqlCommand {
    /** Special value to math all query ids. */
    public static final long ALL_QUERIES = Long.MIN_VALUE;
    /** Node order id. */
    private int nodeOrderId;
    /** Node query id. */
    private long nodeQryId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        SqlGlobalQueryId globalQryId = SqlParserUtils.parseGlobalQueryId(lex);

        nodeOrderId = globalQryId.nodeOrderId();

        nodeQryId = globalQryId.nodeQryId();

        return this;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /**
     * @return Node query id.
     */
    public long getNodeQryId() {
        return nodeQryId;
    }

    /**
     * @return {@code true} in case all queries on a node.
     */
    public boolean isAllQueries() {
        return nodeQryId == ALL_QUERIES;
    }

    /**
     * @return Node order id.
     */
    public int getNodeId() {
        return nodeOrderId;
    }
}
