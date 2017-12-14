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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

/**
 * Asynchronous Checkpointer, encapsulates thread pool functionality and allows
 */
public class AsyncCheckpointer {
    /** Checkpoint runner thread name prefix. */
    public static final String CHECKPOINT_RUNNER = "checkpoint-runner";

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private ExecutorService asyncRunner;

    /**  Number of checkpoint threads. */
    private int checkpointThreads;

    /**
     * @param checkpointThreads Number of checkpoint threads.
     * @param igniteInstanceName Ignite instance name.
     */
    public AsyncCheckpointer(int checkpointThreads, String igniteInstanceName) {
        this.checkpointThreads = checkpointThreads;

        asyncRunner = new IgniteThreadPoolExecutor(
            CHECKPOINT_RUNNER,
            igniteInstanceName,
            checkpointThreads,
            checkpointThreads,
            30_000,
            new LinkedBlockingQueue<Runnable>()
        );
    }

    /**
     * Close async checkpointer, stops all thread from pool
     */
    public void shutdownCheckpointer() {
        asyncRunner.shutdownNow();

        try {
            asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param runnable task to run.
     */
    private void execute(Runnable runnable) {
        try {
            asyncRunner.execute(runnable);
        }
        catch (RejectedExecutionException ignore) {
            // Run the task synchronously.
            runnable.run();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param task task to run.
     * @param doneReportFut Count down future to report this runnable completion.
     */
    public void execute(Callable<Void> task, CountDownFuture doneReportFut) {
        execute(wrapRunnableWithDoneReporting(task, doneReportFut));
    }

    /**
     * @param task actual callable performing required action.
     * @param doneReportFut Count down future to report this runnable completion.
     * @return wrapper runnable which will report result to {@code doneReportFut}
     */
    private static Runnable wrapRunnableWithDoneReporting(final Callable<Void> task,
        final CountDownFuture doneReportFut) {
        return new Runnable() {
            @Override public void run() {
                try {
                    task.call();

                    doneReportFut.onDone((Void)null); // success
                }
                catch (Throwable t) {
                    doneReportFut.onDone(t); //reporting error
                }
            }
        };
    }

    /**
     * @param cpScope Checkpoint scope, contains unsorted collections.
     * @param taskFactory write pages task factory. Should provide callable to write given pages array.
     * @return future will be completed when background writing is done.
     */
    public CountDownFuture quickSortAndWritePages(CheckpointScope cpScope,
        IgniteClosure<FullPageId[], Callable<Void>> taskFactory) {
        // init counter 1 protects here from premature completing
        final CountDownDynamicFuture cntDownDynamicFut = new CountDownDynamicFuture(1);
        FullPageId[] pageIds = cpScope.toArray();

        Callable<Void> task = new QuickSortRecursiveTask(pageIds,
            SEQUENTIAL_CP_PAGE_COMPARATOR,
            taskFactory,
            new IgniteInClosure<Callable<Void>>() {
                @Override public void apply(Callable<Void> call) {
                    fork(call, cntDownDynamicFut);
                }
            },
            checkpointThreads);

        fork(task, cntDownDynamicFut);

        cntDownDynamicFut.onDone((Void)null); //submit of all tasks completed

        return cntDownDynamicFut;
    }

    /**
     * Executes the given runnable in thread pool, registers future to be waited.
     *
     * @param task task to run.
     * @param cntDownDynamicFut Count down future to register job and then report this runnable completion.
     */
    private void fork(Callable<Void> task, CountDownDynamicFuture cntDownDynamicFut) {
        cntDownDynamicFut.incrementTasksCount(); // for created task about to be forked

        execute(task, cntDownDynamicFut);
    }
}
