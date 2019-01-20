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

package org.apache.ignite.console.agent.handlers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;

import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_FAILED;
import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_NODE_REST;
import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_NODE_VISOR_TASK;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestListener extends AbstractVerticle {
    /** */
    private final RestExecutor restExecutor;

    /**
     * @param restExecutor Rest executor.
     */
    public RestListener(RestExecutor restExecutor) {
        this.restExecutor = restExecutor;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        EventBus eventBus = vertx.eventBus();

        eventBus.consumer(EVENT_NODE_REST, this::handleRest);
        eventBus.consumer(EVENT_NODE_VISOR_TASK, this::handleRest);
    }

    /**
     * @param msg Message.
     */
    private void handleRest(Message<JsonObject> msg) {
        vertx.executeBlocking(
            fut -> {
                try {
                    RestResult res = restExecutor.sendRequest(msg.body());

                    fut.complete(res);
                }
                catch (Throwable e) {
                    fut.fail(e);
                }
            },
            asyncRes -> {
                if (asyncRes.succeeded())
                    msg.reply(asyncRes.result());
                else
                    msg.fail(EVENT_FAILED, asyncRes.cause().getMessage());
            });
    }
}
