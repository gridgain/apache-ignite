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
 *
 */

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;

public class H2PkHashIndex extends GridH2IndexBase {

    private final GridH2Table tbl;

    private final GridCacheContext cctx;

    /**
     * @param cctx Cache context.
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param colsList Index columns.
     * @throws IgniteCheckedException If failed.
     */
    public H2PkHashIndex(
        GridCacheContext<?, ?> cctx,
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList
    ) throws IgniteCheckedException {
        assert pk;

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols, IndexType.createPrimaryKey(false, true));

        this.tbl = tbl;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, final SearchRow lower, final SearchRow upper) {
        IndexingQueryFilter f = threadLocalFilter();
        IgniteBiPredicate<Object, Object> p = null;

        if (f != null) {
            String spaceName = getTable().spaceName();

            p = f.forSpace(spaceName);
        }

        KeyCacheObject lowerObj = null;
        KeyCacheObject upperObj = null;

        if (lower != null)
            lowerObj = cctx.toCacheKeyObject(lower.getValue(0).getObject());

        if (upper != null)
            upperObj = cctx.toCacheKeyObject(upper.getValue(0).getObject());

        try {
            List<GridCursor<? extends CacheDataRow>> cursors = new ArrayList<>();

            for (IgniteCacheOffheapManager.CacheDataStore store : cctx.offheap().cacheDataStores()) {
                cursors.add(store.cursor(lowerObj, upperObj));
            }

            return new H2Cursor(new CompositeGridCursor<>(cursors.iterator()), p);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    @Override public boolean canScan() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridH2Row findOne(GridH2Row row) {
        try {
            for (IgniteCacheOffheapManager.CacheDataStore store : cctx.offheap().cacheDataStores()) {
                CacheDataRow found = store.find(row.key);

                if (found != null) {
                    tbl.rowDescriptor().createRow(row.key(), row.partition(), row.value(), row.version(), 0);
                }
            }

            return null;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public GridH2Row put(GridH2Row row) {
        // Should not be called directly. Rows are inserted into underlying cache data stores.

        assert false;

        throw DbException.getUnsupportedException("put");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        // Should not be called directly. Rows are removed from underlying cache data stores.

        assert false;

        throw DbException.getUnsupportedException("remove");
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false);

        int mul = getDistributedMultiplier(ses, filters, filter);

//        return mul * baseCost;

        return Double.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor cursor = find(ses, null, null);

        long res = 0;

        while (cursor.next())
            res++;

        return res;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Object doTakeSnapshot() {
        assert false;

        return this;
    }

    /**
     * Cursor.
     */
    private class H2Cursor implements Cursor {
        /** */
        final GridCursor<? extends CacheDataRow> cursor;

        /** */
        final IgniteBiPredicate<Object, Object> filter;

        /**
         * @param cursor Cursor.
         * @param filter Filter.
         */
        private H2Cursor(GridCursor<? extends CacheDataRow> cursor, IgniteBiPredicate<Object, Object> filter) {
            assert cursor != null;

            this.cursor = cursor;
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            try {
                CacheDataRow dataRow = cursor.get();

                return tbl.rowDescriptor().createRow(dataRow.key(), dataRow.partition(), dataRow.value(), dataRow.version(), 0);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            try {
                while (cursor.next()) {
                    if (filter == null)
                        return true;

                    CacheDataRow dataRow = cursor.get();

                    GridH2Row row = tbl.rowDescriptor().createRow(dataRow.key(), dataRow.partition(), dataRow.value(), dataRow.version(), 0);

                    Object key = row.getValue(0).getObject();
                    Object val = row.getValue(1).getObject();

                    assert key != null;
                    assert val != null;

                    if (filter.apply(key, val))
                        return true;
                }

                return false;
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

    private static class CompositeGridCursor<T> implements GridCursor<T> {
        private final Iterator<GridCursor<? extends T>> iterator;

        private GridCursor<? extends T> current;

        public CompositeGridCursor(Iterator<GridCursor<? extends T>> iterator) {
            this.iterator = iterator;

            if (iterator.hasNext())
                current = iterator.next();
        }

        @Override public boolean next() throws IgniteCheckedException {
            if (current.next())
                return true;

            while (iterator.hasNext()) {
                current = iterator.next();

                if (current.next())
                    return true;
            }

            return false;
        }

        @Override public T get() throws IgniteCheckedException {
            return current.get();
        }
    }
}
