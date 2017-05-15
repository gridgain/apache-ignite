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

package org.apache.ignite.internal.processors.query.schema.operation;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;
import java.util.UUID;

/**
 * Abstract operation on schema.
 */
public abstract class SchemaAbstractOperation implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Operation ID. */
    private final UUID opId;

    /** Space. */
    private final String space;

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param space Space.
     */
    public SchemaAbstractOperation(UUID opId, String space) {
        this.opId = opId;
        this.space = space;
    }

    /**
     * @return Operation id.
     */
    public UUID id() {
        return opId;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaAbstractOperation.class, this);
    }
}
