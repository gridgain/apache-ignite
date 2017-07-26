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

package org.apache.ignite.ml.math;

import org.apache.ignite.ml.math.functions.IgniteFunction;

public class Isomorphism<K, V> {
    private IgniteFunction<K, V> forward;
    private IgniteFunction<V, K> back;

    public static <K> Isomorphism<K, K> id() {
        return new Isomorphism<>(a -> a, a -> a);
    }

    public Isomorphism(IgniteFunction<K, V> forward, IgniteFunction<V, K> back) {
        this.forward = forward;
        this.back = back;
    }

    public V forward(K k) {
        return forward.apply(k);
    }

    public K back(V v) {
        return back.apply(v);
    }
}
