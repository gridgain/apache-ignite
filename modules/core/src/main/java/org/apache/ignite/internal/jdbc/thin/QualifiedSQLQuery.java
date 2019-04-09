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

package org.apache.ignite.internal.jdbc.thin;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Qualified sql query: schema + query itself.
 */
public final class QualifiedSQLQuery {
    /** Schema name. */
    private final String schemaName;

    /** Sql Query. */
    private final String sqlQry;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param sqlQry Sql Query.
     */
    public QualifiedSQLQuery(String schemaName, String sqlQry) {
        this.schemaName = schemaName;
        this.sqlQry = sqlQry;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QualifiedSQLQuery qry = (QualifiedSQLQuery)o;

        return Objects.equals(schemaName, qry.schemaName) &&
            Objects.equals(sqlQry, qry.sqlQry);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        // TODO VO: Calculate hash code manually to avoid array allocation (see Objects.hash signature).
        return Objects.hash(schemaName, sqlQry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QualifiedSQLQuery.class, this);
    }
}
