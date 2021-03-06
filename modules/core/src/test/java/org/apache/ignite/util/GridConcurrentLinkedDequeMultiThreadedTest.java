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

package org.apache.ignite.util;

import org.apache.ignite.internal.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jdk8.backport.ConcurrentLinkedDeque8.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for {@link ConcurrentLinkedDeque8}.
 */
public class GridConcurrentLinkedDequeMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final Random RND = new Random();

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void testQueueMultiThreaded() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final ConcurrentLinkedDeque8<Byte> queue = new ConcurrentLinkedDeque8<>();

        // Poll thread.
        IgniteInternalFuture<?> pollFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get())
                        try {
                            queue.poll();
                        }
                        catch (Throwable t) {
                            error("Error in poll thread.", t);

                            done.set(true);
                        }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-poll"
        );

        // Producer thread.
        IgniteInternalFuture<?> prodFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get()) {
                        Node<Byte> n = queue.addx((byte)1);

                        if (RND.nextBoolean())
                            queue.unlinkx(n);
                    }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-prod"
        );

        Thread.sleep(2 * 60 * 1000);


        done.set(true);

        pollFut.get();
        prodFut.get();
    }
}
