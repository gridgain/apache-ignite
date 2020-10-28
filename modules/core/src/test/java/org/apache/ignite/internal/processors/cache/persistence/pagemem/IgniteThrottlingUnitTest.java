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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.Thread.State.TIMED_WAITING;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteThrottlingUnitTest extends GridCommonAbstractTest {
    /** Logger. */
    private IgniteLogger log = new NullLogger();

    /** Page memory 2 g. */
    private PageMemoryImpl pageMemory2g = mock(PageMemoryImpl.class);

    /** State checker. */
    private CheckpointLockStateChecker stateChecker = () -> true;

    {
        when(pageMemory2g.totalPages()).thenReturn((2L * 1024 * 1024 * 1024) / 4096);
    }

    /**
     *
     */
    public void testBreakInCaseTooFast() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, log);

        long time = throttle.getParkTime(0.67,
            (362584 + 67064) / 2,
            328787,
            1,
            60184,
            23103);

        assertTrue(time > 0);
    }

    /**
     *
     */
    public void testNoBreakIfNotFastWrite() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, log);

        long time = throttle.getParkTime(0.47,
            ((362584 + 67064) / 2),
            328787,
            1,
            20103,
            23103);

        assertTrue(time == 0);
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    public void testAverageCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++)
            measurement.addMeasurementForAverageCalculation(100);

        assertEquals(100, measurement.getAverage());

        Thread.sleep(220);

        assertEquals(0, measurement.getAverage());

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    public void testSpeedCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++)
            measurement.setCounter(i, System.nanoTime());

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());
        System.out.println("speed measured " + speed);
        assertTrue(speed > 1000);

        Thread.sleep(230);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    public void testSpeedWithDelayCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        int runs = 10;
        int nanosPark = 100;
        int multiplier = 100000;
        for (int i = 0; i < runs; i++) {
            measurement.setCounter(i * multiplier, System.nanoTime());

            LockSupport.parkNanos(nanosPark);
        }

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());

        assertTrue(speed > 0);
        long maxSpeed = (TimeUnit.SECONDS.toNanos(1) * multiplier * runs) / ((long)(runs * nanosPark));
        assertTrue(speed < maxSpeed);

        Thread.sleep(200);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     *
     */
    public void testBeginOfCp() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, log);

        assertTrue(throttle.getParkTime(0.01, 100, 400000,
            1,
            20103,
            23103) == 0);

        //mark speed 22413 for mark all remaining as dirty
        long time = throttle.getParkTime(0.024, 100, 400000,
            1,
            24000,
            23103);
        assertTrue(time > 0);

        assertTrue(throttle.getParkTime(0.01,
            100,
            400000,
            1,
            22412,
            23103) == 0);
    }

    /**
     *
     */
    public void testEnforceThrottleAtTheEndOfCp() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, log);

        long time1 = throttle.getParkTime(0.70, 300000, 400000,
            1, 20200, 23000);
        long time2 = throttle.getParkTime(0.71, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time2 >= time1 * 2); // extra slowdown should be applied.

        long time3 = throttle.getParkTime(0.73, 300000, 400000,
            1, 20200, 23000);
        long time4 = throttle.getParkTime(0.74, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time3 > time2);
        assertTrue(time4 > time3);
    }

    /**
     *
     */
    public void testTooMuchPagesMarkedDirty() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, log);

        // 363308	350004	348976	10604
        long time = throttle.getParkTime(0.75,
            ((350004 + 348976) / 2),
            350004 - 10604,
            4,
            279,
            23933);

        System.err.println(time);

        assertTrue(time == 0);
    }

    /**
     * @throws IgniteInterruptedCheckedException if fail.
     */
    @Test
    public void testWakeupSpeedBaseThrottledThreadOnCheckpointFinish() throws IgniteInterruptedCheckedException {
        //given: Enabled throttling with EXPONENTIAL level.
        CheckpointWriteProgressSupplier cpProgress = mock(CheckpointWriteProgressSupplier.class);
        when(cpProgress.writtenPagesCounter()).thenReturn(new AtomicInteger(200));

        PagesWriteThrottlePolicy plc = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProgress, stateChecker, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                //Force parking to long time.
                super.doPark(TimeUnit.SECONDS.toNanos(10));
            }
        };

        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(100);
        when(pageMemory2g.checkpointBufferPagesCount()).thenAnswer(mock -> new AtomicInteger(70));

        AtomicBoolean stopLoad = new AtomicBoolean();
        List<Thread> loadThreads = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            loadThreads.add(new Thread(
                () -> {
                    while (!stopLoad.get())
                        plc.onMarkDirty(true);
                },
                "load-" + i
            ));
        }

        try {
            loadThreads.forEach(Thread::start);

            //and: All load threads are parked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() == TIMED_WAITING, 1000L));

            //when: Disable throttling
            when(cpProgress.writtenPagesCounter()).thenReturn(null);

            //and: Finish the checkpoint.
            plc.onFinishCheckpoint();

            //then: All load threads should be unparked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() != TIMED_WAITING, 500L));

            for (Thread t : loadThreads)
                assertNotEquals(t.getName(), TIMED_WAITING, t.getState());
        }
        finally {
            stopLoad.set(true);
        }
    }

    /**
     *
     */
    @Test
    public void testWakeupThrottledThread() throws IgniteInterruptedCheckedException, InterruptedException {
        PagesWriteThrottlePolicy plc = new PagesWriteThrottle(pageMemory2g, null, stateChecker, true, log);

        AtomicBoolean stopLoad = new AtomicBoolean();
        List<Thread> loadThreads = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            loadThreads.add(new Thread(
                () -> {
                    while (!stopLoad.get())
                        plc.onMarkDirty(true);
                },
                "load-" + i
            ));
        }

        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(100);

        AtomicInteger checkpointBufferPagesCount = new AtomicInteger(70);

        when(pageMemory2g.checkpointBufferPagesCount()).thenAnswer(mock -> checkpointBufferPagesCount.get());

        try {
            loadThreads.forEach(Thread::start);

            for (int i = 0; i < 1_000; i++)
                loadThreads.forEach(LockSupport::unpark);

            // Awaiting that all load threads are parked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() == TIMED_WAITING, 500L));

            // Disable throttling
            checkpointBufferPagesCount.set(50);

            // Awaiting that all load threads are unparked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() != TIMED_WAITING, 500L));

            for (Thread t : loadThreads)
                assertNotEquals(t.getName(), TIMED_WAITING, t.getState());
        }
        finally {
            stopLoad.set(true);
        }
    }

    /**
     *
     */
    public void testWarningInCaseTooMuchThrottling() {
        AtomicInteger warnings = new AtomicInteger(0);
        IgniteLogger log = mock(IgniteLogger.class);

        when(log.isInfoEnabled()).thenReturn(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();

            System.out.println("log.info() called with arguments: " + Arrays.toString(args));

            warnings.incrementAndGet();

            return null;
        }).when(log).info(anyString());

        AtomicInteger written = new AtomicInteger();
        CheckpointWriteProgressSupplier cpProgress = mock(CheckpointWriteProgressSupplier.class);
        when(cpProgress.writtenPagesCounter()).thenReturn(written);

        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProgress, stateChecker, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                //do nothing
            }
        };
        throttle.onBeginCheckpoint();
        written.set(200); //emulating some pages written

        for (int i = 0; i < 100000; i++) {
            //emulating high load on marking
            throttle.onMarkDirty(false);

            if (throttle.throttleWeight() > PagesWriteSpeedBasedThrottle.WARN_THRESHOLD)
                break;
        }

        for (int i = 0; i < 1000; i++) {
            //emulating additional page writes to be sure log message is generated

            throttle.onMarkDirty(false);

            if (warnings.get() > 0)
                break;
        }

        System.out.println(throttle.throttleWeight());

        assertTrue(warnings.get() > 0);
    }
}
