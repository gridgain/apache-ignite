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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link GridHandleTable}.
 */
public class GridIntSetSelfTest extends GridCommonAbstractTest {
    /**
     * Tests set grow.
     */
    public void testSegment() {
        for (short step = 1; step <= 5; step++) {
            GridIntSet set = new GridIntSet();

            short size = 1;
            for (short i = 0; i < GridIntSet.THRESHOLD2; i += step, size += 1) {
                set.add(i);

                testPredicates(set, size);
            }

            for (short i = 0; i < set.size(); i ++)
                assertEquals("Contains: " + i, i % step == 0, set.contains(i));
        }
    }

    /** */
    private void testPredicates(GridIntSet set, short expSize) {
        GridIntSet.Segment seg = segment(set, 0);

        if (set.size() <= GridIntSet.THRESHOLD)
            assertTrue(seg instanceof GridIntSet.ArraySegment);
        else if (set.size() > GridIntSet.THRESHOLD2)
            assertTrue(seg instanceof GridIntSet.InvertedArraySegment);
        else
            assertTrue(seg.getClass().toString(), seg instanceof GridIntSet.BitSetSegment);

        assertEquals(expSize, seg.size());

        // New length is upper power of two.
        assertEquals(seg.data().length, 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(seg.used() - 1)));
    }

    /** */
    private GridIntSet.Segment segment(GridIntSet set, int idx) {
        GridIntSet.Segment[] segments = U.field(set, "segments");

        return segments[idx];
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdd() throws Exception {
//        GridIntSet set = new GridIntSet();
//
//        for (int i = 0; i < Short.SIZE; i++) {
//            set.add(i);
//
//            short[][] segments = U.field(set, "segments");
//
//            assertEquals("Words used", 1, segments[0].length);
//        }
//
//        set.dump();
//
//        set.add(60);
//        set.dump();
//
//        short tmp = -1;
//
//        System.out.println(Integer.toBinaryString(tmp));
    }
}
