/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;

/**
 * Abstract class for all control.sh commands, has already implemented methods and abstract methods.
 * Define flow how to work with command.
 *
 * @param <T> Generic for getArg method which should return command-specific paramters which it would be run with.
 */
public abstract class Command<T> {
    public abstract Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception;

    protected final GridClient startClient(GridClientConfiguration clientCfg) throws Exception {
        GridClient client = GridClientFactory.start(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected())
            client.throwLastError();

        return client;
    }

    protected String confirmationPrompt0() {
        return null;
    }

    public void parseArguments(CommandArgIterator argIterator) {
        //Empty block.
    }

    public final String confirmationPrompt() {
        return confirmationPrompt0();
    }

    public abstract T arg();
}
