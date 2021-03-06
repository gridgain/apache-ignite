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

package org.apache.ignite.internal.util.typedef;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.lang.*;

/**
 * Defines {@code alias} for <tt>GridPredicate&lt;GridNode&gt;</tt> by extending
 * {@link org.apache.ignite.lang.IgnitePredicate}. Since Java doesn't provide type aliases (like Scala, for example) we resort
 * to these types of measures. This is intended to provide for more concise code without sacrificing
 * readability. For more information see {@link org.apache.ignite.lang.IgnitePredicate} and {@link org.apache.ignite.cluster.ClusterNode}.
 * @see org.apache.ignite.lang.IgnitePredicate
 * @see org.apache.ignite.cluster.ClusterNode
 * @see GridFunc
 */
public interface PN extends IgnitePredicate<ClusterNode> { /* No-op. */ }
