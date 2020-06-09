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
package org.apache.ignite.compatibility.sql.randomsql;

/**
 * Base class for AST
 */
public abstract class Ast {

    private final Ast parent;

    private final Scope scope;

    private final int level;


    public Ast(Ast parent, Scope scope) {
        this.scope = scope;
        if (parent == null) {
            this.parent = null;
            level = 0;
        }
        else {
            this.parent = parent;
            level = parent.level + 1;
        }
    }

}
