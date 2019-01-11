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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.stat.IoStatisticsType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class H2TreeIndex extends H2TreeIndexBase {
    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /** */
    private final H2Tree[] segments;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Table name. */
    private final String tblName;

    /** */
    private final boolean pk;

    /** */
    private final boolean affinityKey;

    /** */
    private final String idxName;

    /** Tree name. */
    private final String treeName;

    /** */
    private final IgniteLogger log;

    /**
     * @param cctx Cache context.
     * @param rowCache Row cache.
     * @param tbl Table.
     * @param idxName Index name.
     * @param pk Primary key.
     * @param affinityKey {@code true} for affinity key.
     * @throws IgniteCheckedException If failed.
     */
    private H2TreeIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table tbl,
        String idxName,
        boolean pk,
        boolean affinityKey,
        IndexColumnsInfo idxColsInfo,
        String treeName,
        H2Tree[] segments
    ) throws IgniteCheckedException {
        super(tbl, 0, idxName,
            idxColsInfo.cols(),
            pk ? IndexType.createPrimaryKey(false, false) :
                IndexType.createNonUnique(false, false, false));

        this.cctx = cctx;

        this.log = cctx.logger(getClass().getName());

        this.pk = pk;
        this.affinityKey = affinityKey;

        this.tblName = tbl.getName();
        this.idxName = idxName;

        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        this.treeName = treeName;

        this.segments = segments;

        inlineIdxs = idxColsInfo.inlineIdx();

        IndexColumn.mapColumns(idxColsInfo.cols(), tbl);

        initDistributedJoinMessaging(tbl);
    }

    /**
     * @param cctx Cache context.
     * @param rowCache Row cache.
     * @param tbl Table.
     * @param idxName Index name.
     * @param pk Primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param unwrappedColsList Unwrapped index columns for complex types.
     * @param wrappedColsList Index columns as is.
     * @param inlineSize Inline size.
     * @param segmentsCnt Count of tree segments.
     * @throws IgniteCheckedException If failed.
     */
    public static H2TreeIndex createIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table tbl,
        String idxName,
        boolean pk,
        boolean affinityKey,
        List<IndexColumn> unwrappedColsList,
        List<IndexColumn> wrappedColsList,
        int inlineSize,
        int segmentsCnt,
        IgniteLogger log
    ) throws IgniteCheckedException {
        assert segmentsCnt > 0 : segmentsCnt;

        IndexColumnsInfo unwrappedColsInfo = new IndexColumnsInfo(
            unwrappedColsList,
            inlineSize,
            affinityKey,
            cctx,
            idxName,
            log,
            pk,
            tbl);

        IndexColumnsInfo wrappedColsInfo = new IndexColumnsInfo(
            wrappedColsList,
            inlineSize,
            affinityKey,
            cctx,
            idxName,
            log,
            pk,
            tbl);

        GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        String treeName = BPlusTree.treeName((tbl.rowDescriptor() == null ? "" : typeId + "_") + idxName, "H2Tree");

        assert cctx.affinityNode();

        H2Tree[] segments = new H2Tree[segmentsCnt];

        IgniteCacheDatabaseSharedManager db = cctx.shared().database();

        AtomicInteger maxCalculatedInlineSize = new AtomicInteger();

        IoStatisticsHolder stats = cctx.kernalContext().ioStats().register(
            IoStatisticsType.SORTED_INDEX,
            cctx.name(),
            idxName
        );

        for (int i = 0; i < segments.length; i++) {
            db.checkpointReadLock();

            try {
                RootPage page = getMetaPage(cctx, treeName, i);

                segments[i] = new H2Tree(
                    treeName,
                    idxName,
                    tbl.getName(),
                    tbl.cacheName(),
                    cctx.offheap().reuseListForIndex(treeName),
                    cctx.groupId(),
                    cctx.dataRegion().pageMemory(),
                    cctx.shared().wal(),
                    cctx.offheap().globalRemoveId(),
                    tbl.rowFactory(),
                    page.pageId().pageId(),
                    page.isAllocated(),
                    unwrappedColsInfo,
                    wrappedColsInfo,
                    maxCalculatedInlineSize,
                    pk,
                    affinityKey,
                    cctx.mvccEnabled(),
                    rowCache,
                    cctx.kernalContext().failure(),
                    log,
                    stats) {
                    @Override public int compareValues(Value v1, Value v2) {
                        return v1 == v2 ? 0 : tbl.compareValues(v1, v2);
                    }
                };
            }
            finally {
                db.checkpointReadUnlock();
            }
        }

        boolean useUnwrappedCols = segments[0].unwrappedPk();

        IndexColumnsInfo idxColsInfo = useUnwrappedCols ? unwrappedColsInfo : wrappedColsInfo;

        return  new H2TreeIndex(cctx, rowCache, tbl, idxName, pk, affinityKey, idxColsInfo, treeName, segments);
    }

    /**
     * Check if index exists in store.
     *
     * @return {@code True} if exists.
     */
    public boolean rebuildRequired() {
        assert segments != null;

        for (int i = 0; i < segments.length; i++) {
            try {
                H2Tree segment = segments[i];

                if (segment.created())
                    return true;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to check index tree root page existence [cacheName=" + cctx.name() +
                    ", tblName=" + tblName + ", idxName=" + idxName + ", segment=" + i + ']');
            }
        }

        return false;
    }

    /**
     * @param affinityKey {@code true} for affinity key.
     * @param cctx Cache context.
     * @param idxName Index name.
     * @param log Logger.
     * @param pk PK flag.
     * @param tbl table.
     * @param cols Columns array.
     * @return List of {@link InlineIndexHelper} objects.
     */
    private static List<InlineIndexHelper> getAvailableInlineColumns(boolean affinityKey, GridCacheContext<?, ?> cctx,
        String idxName, IgniteLogger log, boolean pk, Table tbl, IndexColumn[] cols) {
        List<InlineIndexHelper> res = new ArrayList<>();

        for (IndexColumn col : cols) {
            if (!InlineIndexHelper.AVAILABLE_TYPES.contains(col.column.getType())) {
                String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

                U.warn(log, "Column cannot be inlined into the index because it's type doesn't support inlining, " +
                    "index access may be slow due to additional page reads (change column type if possible) " +
                    "[cacheName=" + cctx.name() +
                    ", tableName=" + tbl.getName() +
                    ", idxName=" + idxName +
                    ", idxType=" + idxType +
                    ", colName=" + col.columnName +
                    ", columnType=" + InlineIndexHelper.nameTypeBycode(col.column.getType()) + ']'
                );

                break;
            }

            InlineIndexHelper idx = new InlineIndexHelper(
                col.columnName,
                col.column.getType(),
                col.column.getColumnId(),
                col.sortType,
                tbl.getCompareMode());

            res.add(idx);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments.length;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        assert lower == null || lower instanceof GridH2SearchRow : lower;
        assert upper == null || upper instanceof GridH2SearchRow : upper;

        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            if (!cctx.mvccEnabled() && indexType.isPrimaryKey() && lower != null && upper != null &&
                tree.compareRows((GridH2SearchRow)lower, (GridH2SearchRow)upper) == 0) {
                GridH2Row row = tree.findOne((GridH2SearchRow)lower, filter(GridH2QueryContext.get()), null);

                return (row == null) ? GridH2Cursor.EMPTY : new SingleRowCursor(row);
            }
            else {
                return new H2Cursor(tree.find((GridH2SearchRow)lower,
                    (GridH2SearchRow)upper, filter(GridH2QueryContext.get()), null));
            }
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.putx(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        assert row instanceof GridH2SearchRow : row;

        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.remove((GridH2SearchRow)row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        assert row instanceof GridH2SearchRow : row;

        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.removex((GridH2SearchRow)row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        AllColumnsForPlan allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            GridH2QueryContext qctx = GridH2QueryContext.get();

            return tree.size(filter(qctx));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        try {
            H2Tree tree = treeForRead(threadLocalSegment());
            GridH2QueryContext qctx = GridH2QueryContext.get();

            return new SingleRowCursor(b ? tree.findFirst(filter(qctx)): tree.findLast(filter(qctx)));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmvIdx) {
        try {
            if (cctx.affinityNode() && rmvIdx) {
                assert cctx.shared().database().checkpointLockIsHeldByThread();

                for (int i = 0; i < segments.length; i++) {
                    H2Tree tree = segments[i];

                    tree.destroy();

                    dropMetaPage(i);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            super.destroy(rmvIdx);
        }
    }

    /** {@inheritDoc} */
    @Override protected H2Tree treeForRead(int segment) {
        return segments[segment];
    }

    /** {@inheritDoc} */
    @Override protected BPlusTree.TreeRowClosure<GridH2SearchRow, GridH2Row> filter(GridH2QueryContext qctx) {
        if (qctx == null) {
            assert !cctx.mvccEnabled();

            return null;
        }

        IndexingQueryFilter f = qctx.filter();
        IndexingQueryCacheFilter p = f == null ? null : f.forCache(getTable().cacheName());
        MvccSnapshot v = qctx.mvccSnapshot();

        assert !cctx.mvccEnabled() || v != null;

        if(p == null && v == null)
            return null;

        return new H2TreeFilterClosure(p, v, cctx, log);
    }

    /**
     * @param cctx Cache context.
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from cache config.
     * @return Inline size.
     */
    private static int computeInlineSize(GridCacheContext<?, ?> cctx, List<InlineIndexHelper> inlineIdxs,
        int cfgInlineSize) {
        int confSize = cctx.config().getSqlIndexMaxInlineSize();

        int propSize = confSize == -1 ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE,
            IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT) : confSize;

        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize == -1) {
            if (propSize == 0)
                return 0;

            int size = 0;

            for (InlineIndexHelper idxHelper : inlineIdxs) {
                if (idxHelper.size() <= 0) {
                    size = propSize;
                    break;
                }
                // 1 byte type + size
                size += idxHelper.size() + 1;
            }

            return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
        }
        else
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);
    }

    /**
     * @param cctx Cache context.
     * @param treeName Tree name.
     * @param segIdx Segment index.
     * @return RootPage for meta page.
     * @throws IgniteCheckedException If failed.
     */
    private static RootPage getMetaPage(GridCacheContext<?, ?> cctx, String treeName, int segIdx)
        throws IgniteCheckedException {
        return cctx.offheap().rootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /**
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(int segIdx) throws IgniteCheckedException {
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        super.refreshColumnIds();

        if (inlineIdxs == null)
            return;

        List<InlineIndexHelper> inlineHelpers = getAvailableInlineColumns(
            affinityKey, cctx, idxName, log, pk, table, indexColumns);

        assert inlineIdxs.size() == inlineHelpers.size();

        for (int pos = 0; pos < inlineHelpers.size(); ++pos)
            inlineIdxs.set(pos, inlineHelpers.get(pos));
    }

    /**
     *
     */
    public static class IndexColumnsInfo {
        /** */
        private final int inlineSize;
        /** */
        private final IndexColumn[] cols;
        /** */
        private final List<InlineIndexHelper> inlineIdx;

        /**
         * @param colsList Index columns list
         * @param cfgInlineSize Inline size from cache config.
         */
        public IndexColumnsInfo(List<IndexColumn> colsList, int cfgInlineSize,
            boolean affinityKey, GridCacheContext<?, ?> cctx,
            String idxName, IgniteLogger log, boolean pk, Table tbl) {
            this.cols = colsList.toArray(new IndexColumn[0]);

            inlineIdx = getAvailableInlineColumns(affinityKey, cctx, idxName, log, pk, tbl, cols);

            inlineSize = computeInlineSize(cctx, inlineIdx, cfgInlineSize);
        }

        /**
         * @return Inline size.
         */
        public int inlineSize() {
            return inlineSize;
        }

        /**
         * @return Index columns.
         */
        public IndexColumn[] cols() {
            return cols;
        }

        /**
         * @return Inline indexes.
         */
        public List<InlineIndexHelper> inlineIdx() {
            return inlineIdx;
        }
    }
}
