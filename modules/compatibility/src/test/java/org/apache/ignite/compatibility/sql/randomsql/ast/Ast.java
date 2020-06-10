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
package org.apache.ignite.compatibility.sql.randomsql.ast;

import java.util.Random;
import org.apache.ignite.compatibility.sql.randomsql.Scope;

/**
 * Base class for AST
 */
public abstract class Ast {

    private final Ast parent;

    private Scope scope;

    private final int level;

    private Random rnd;


    public Ast(Ast parent) {
        if (parent == null) {
            this.parent = null;
            level = 0;
            scope = null;
        }
        else {
            this.parent = parent;
            level = parent.level + 1;
            scope = parent.scope;
            rnd = parent.rnd;
        }
    }

    public abstract void print(StringBuilder out);

    public Random random() {
        return rnd;
    }

    public void init(int seed, Scope scope) {
        rnd = new Random(seed);
        this.scope = scope;
        this.scope.setRandom(rnd);
    }

    public Scope scope() {
        return scope;
    }
}
