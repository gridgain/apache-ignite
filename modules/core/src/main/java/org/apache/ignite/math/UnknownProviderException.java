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

package org.apache.ignite.math;

import org.apache.ignite.*;

/**
 * Indicates that no provider has been found for a given vector or matrix flavor.
 */
public class UnknownProviderException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     * @param flavor Flavor (a.k.a. operation performance hints) that has no registered provider for.
     */
    public UnknownProviderException(String flavor) {
        super("No provider has been found for the flavor: " + flavor);
    }
}
