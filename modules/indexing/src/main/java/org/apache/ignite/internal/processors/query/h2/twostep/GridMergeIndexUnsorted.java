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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.h2.index.*;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Unsorted merge index.
 */
public class GridMergeIndexUnsorted extends GridMergeIndex {
    /** */
    private final BlockingQueue<GridResultPage> queue = new LinkedBlockingQueue<>();

    /**
     * @param tbl  Table.
     * @param name Index name.
     */
    public GridMergeIndexUnsorted(GridMergeTable tbl, String name) {
        super(tbl, name, IndexType.createScan(false), IndexColumn.wrap(tbl.getColumns()));
    }

    /** {@inheritDoc} */
    @Override protected void addPage0(GridResultPage page) {
        if (!page.rows().isEmpty() || page.isLast() || queue.isEmpty())
            queue.add(page);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first, @Nullable SearchRow last) {
        return new IteratorCursor(fetched.iterator());
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        return new FetchingCursor(new Iterator<Row>() {
            /** */
            Iterator<Value[]> iter = Collections.emptyIterator();

            @Override public boolean hasNext() {
                while (!iter.hasNext()){
                    GridResultPage page;

                    try {
                        page = queue.take();
                    }
                    catch (InterruptedException e) {
                        throw new CacheException("Query execution was interrupted.", e);
                    }

                    if (page.isLast())
                        return false; // We are done.

                    fetchNextPage(page);

                    iter = page.rows().iterator();
                }

                return true;
            }

            @Override public Row next() {
                return new Row(iter.next(), 0);
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }
}
