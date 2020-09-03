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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.batchRenameDefragmentedCacheGroupPartitions;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedIndexTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartMappingFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedCacheGroup;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedPartition;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.writeDefragmentationCompletionMarker;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator.PageAccessType.ACCESS_READ;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator.PageAccessType.ACCESS_WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator.access;

/** */
public class CachePartitionDefragmentationManager {
    /** */
    @Deprecated public static final String DEFRAGMENTATION = "DEFRAGMENTATION";

    /** */
    @Deprecated public static final String SKIP_CP_ENTRIES = "SKIP_CP_ENTRIES";

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> sharedCtx;

    /** Defragmentation context. */
    private final CacheDefragmentationContext defrgCtx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param sharedCtx Cache shared context.
     * @param defrgCtx Defragmentation context.
     */
    public CachePartitionDefragmentationManager(
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheDefragmentationContext defrgCtx
    ) {
        this.sharedCtx = sharedCtx;
        this.defrgCtx = defrgCtx;

        log = sharedCtx.logger(getClass());
    }

    /** */
    public void executeDefragmentation() throws IgniteCheckedException {
        int pageSize = sharedCtx.gridConfig().getDataStorageConfiguration().getPageSize();

        TreeIterator treeIterator = new TreeIterator(pageSize);

        System.setProperty(SKIP_CP_ENTRIES, "true");

        try {
            FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)sharedCtx.pageStore();

            DataRegion partRegion = defrgCtx.partitionsDataRegion();

            for (int grpId : defrgCtx.groupIdsForDefragmentation()) {
                File workDir = defrgCtx.workDirForGroupId(grpId);

                if (skipAlreadyDefragmentedCacheGroup(workDir, grpId, log))
                    continue;

                int[] parts = defrgCtx.partitionsForGroupId(grpId);

                if (workDir != null && parts != null) {
                    CacheGroupContext grpCtx = defrgCtx.groupContextByGroupId(grpId);

                    GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)sharedCtx.database();

                    GridCacheOffheapManager offheap = (GridCacheOffheapManager)grpCtx.offheap();

                    // A cheat so that we won't try to save old metadata into a new partition.
                    dbMgr.removeCheckpointListener(offheap);

                    // Another cheat. Ttl cleanup manager knows too much shit.
                    grpCtx.caches().stream()
                        .filter(cacheCtx -> cacheCtx.groupId() == grpId)
                        .forEach(cacheCtx -> cacheCtx.ttl().unregister());

                    // Technically wal is already disabled, but PageHandler.isWalDeltaRecordNeeded doesn't care and
                    // WAL records will be allocated anyway just to be ignored later if we don't disable WAL for
                    // cache group explicitly.
                    grpCtx.localWalEnabled(false, false);

                    boolean encrypted = grpCtx.config().isEncryptionEnabled();

                    FilePageStoreFactory pageStoreFactory = filePageStoreMgr.getPageStoreFactory(grpId, encrypted);

                    // Index partition file has to be deleted before we begin, otherwise there's a chance of reading
                    // corrupted file.
                    // There is a time period when index is already defragmented but marker file is not created yet.
                    // If node is failed in that time window then index will be deframented once again. That's fine,
                    // situation is rare but code to fix that would add unnecessary complications.
                    U.delete(defragmentedIndexTmpFile(workDir));

                    PageStore idxPageStore = pageStoreFactory.createPageStore(
                        FLAG_IDX,
                        () -> defragmentedIndexTmpFile(workDir).toPath(),
                        val -> {}
                    );

                    idxPageStore.sync();

                    defrgCtx.addPartPageStore(grpId, PageIdAllocator.INDEX_PARTITION, idxPageStore);

                    GridCompoundFuture<Object, Object> cmpFut = new GridCompoundFuture<>();

                    PageMemoryEx oldPageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

                    Map<Integer, LinkMap> mappingByPartition = new HashMap<>();

                    for (int partId : parts) {
                        if (skipAlreadyDefragmentedPartition(workDir, grpId, partId, log)) {
                            createMappingPageStore(grpId, workDir, pageStoreFactory, partId);

                            DataRegion mappingRegion = defrgCtx.mappingDataRegion();

                            PageMemory memory = mappingRegion.pageMemory();

                            sharedCtx.database().checkpointReadLock(); //TODO We should have many small checkpoints.
                            try {
                                FullPageId linkMapMetaPageId = new FullPageId(PageIdUtils.pageId(partId, FLAG_DATA, 2), grpId);

                                LinkMap existingMapping = new LinkMap(grpCtx, memory, linkMapMetaPageId.pageId(), false);

                                mappingByPartition.put(partId, existingMapping);
                            }
                            finally {
                                sharedCtx.database().checkpointReadUnlock();
                            }

                            continue;
                        }

                        AtomicLong partPagesAllocated = new AtomicLong();

                        //TODO I think we should do it inside of checkpoint read lock.
                        PageStore partPageStore = pageStoreFactory.createPageStore(
                            FLAG_DATA,
                            () -> defragmentedPartTmpFile(workDir, partId).toPath(),
                            partPagesAllocated::addAndGet
                        );

                        partPageStore.sync();

                        defrgCtx.addPartPageStore(grpId, partId, partPageStore);

                        AtomicLong mappingPagesAllocated = createMappingPageStore(grpId, workDir, pageStoreFactory, partId);

                        sharedCtx.database().checkpointReadLock(); //TODO We should have many small checkpoints.

                        try {
                            LinkMap map = defragmentSinglePartition(grpCtx, partId, treeIterator, pageSize);
                            mappingByPartition.put(partId, map);
                        }
                        finally {
                            sharedCtx.database().checkpointReadUnlock();
                        }

                        //TODO Move inside of defragmentSinglePartition, get rid of that ^ stupid checkpoint read lock.
                        IgniteInClosure<IgniteInternalFuture<?>> cpLsnr = fut -> {
                            if (fut.error() == null) {
                                // TODO: This dirty hack is needed (for now) for index defragmentation
                                // oldPageMem.invalidate(grpId, partId);

                                ((PageMemoryEx)partRegion.pageMemory()).invalidate(grpId, partId);

                                renameTempPartitionFile(workDir, partId);

                                log.info(S.toString(
                                    "Partition defragmented",
                                    "grpId", grpId, false,
                                    "partId", partId, false,
                                    "oldPages", defrgCtx.pageStore(grpId, partId).pages(), false,
                                    "newPages", partPagesAllocated.get(), false,
                                    "bytesSaved", (defrgCtx.pageStore(grpId, partId).pages() - partPagesAllocated.get()) * pageSize, false,
                                    "mappingPages", mappingPagesAllocated.get(), false,
                                    "partFile", defragmentedPartFile(workDir, partId).getName(), false,
                                    "workDir", workDir, false
                                ));
                            }
                        };

                        GridFutureAdapter<?> cpFut = sharedCtx.database()
                            .forceCheckpoint("part-" + partId) //TODO Provide a good reason.
                            .futureFor(CheckpointState.FINISHED);

                        cpFut.listen(cpLsnr);

                        cmpFut.add((IgniteInternalFuture<Object>)cpFut);
                    }

                    // A bit too general for now, but I like it more then saving only the last checkpoint future.
                    cmpFut.markInitialized().get();

                    if (sharedCtx.pageStore().hasIndexStore(grpId)) {
                        sharedCtx.database().checkpointReadLock(); //TODO We should have many small checkpoints.

                        try {
                            defragmentIndexPartition(grpCtx, mappingByPartition);
                        }
                        finally {
                            sharedCtx.database().checkpointReadUnlock();
                        }
                    }
                    idxPageStore.sync();

                    sharedCtx.database()
                            .forceCheckpoint("index") //TODO Provide a good reason.
                            .futureFor(CheckpointState.FINISHED).get();

                    oldPageMem.invalidate(grpId, PageIdAllocator.INDEX_PARTITION);
                    ((PageMemoryEx)partRegion.pageMemory()).invalidate(grpId, PageIdAllocator.INDEX_PARTITION);

                    renameTempIndexFile(workDir);

                    writeDefragmentationCompletionMarker(filePageStoreMgr.getPageStoreFileIoFactory(), workDir, log);

                    batchRenameDefragmentedCacheGroupPartitions(workDir, log);

                    defrgCtx.onCacheGroupDefragmented(grpId);
                }
            }
        }
        finally {
            System.clearProperty(SKIP_CP_ENTRIES);
        }
    }

    /**
     * Create page store for link mapping.
     *
     * @param grpId
     * @param workDir
     * @param pageStoreFactory
     * @param partId
     *
     * @throws IgniteCheckedException If failed.
     * @return
     */
    public AtomicLong createMappingPageStore(
        int grpId,
        File workDir,
        FilePageStoreFactory pageStoreFactory,
        int partId
    ) throws IgniteCheckedException {
        AtomicLong mappingPagesAllocated = new AtomicLong();

        PageStore mappingPageStore = pageStoreFactory.createPageStore(
                FLAG_DATA,
                () -> defragmentedPartMappingFile(workDir, partId).toPath(),
                mappingPagesAllocated::addAndGet
        );

        mappingPageStore.sync();

        defrgCtx.addMappingPageStore(grpId, partId, mappingPageStore);

        return mappingPagesAllocated;
    }

    /**
     * Defragmentate partition ang get link mapping (from old link to new link).
     *
     * @param grpCtx
     * @param partId
     * @param treeIterator
     * @param pageSize
     *
     * @return Link mapping.
     * @throws IgniteCheckedException If failed.
     */
    private LinkMap defragmentSinglePartition(
        CacheGroupContext grpCtx,
        int partId,
        TreeIterator treeIterator,
        int pageSize
    ) throws IgniteCheckedException {
        DataRegion partRegion = defrgCtx.partitionsDataRegion();
        PageMemoryEx partPageMem = (PageMemoryEx)partRegion.pageMemory();

        DataRegion mappingRegion = defrgCtx.mappingDataRegion();

        PageMemoryEx cachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        int grpId = grpCtx.groupId();

        CacheGroupContext newCtx = new CacheGroupContext(
            sharedCtx,
            grpId,
            grpCtx.receivedFrom(),
            CacheType.USER,
            grpCtx.config(),
            grpCtx.affinityNode(),
            partRegion,
            grpCtx.cacheObjectContext(),
            null,
            null,
            grpCtx.localStartVersion(),
            true,
            false,
            true
        );

        newCtx.start();

        GridCacheOffheapManager.GridCacheDataStore newCacheDataStore = new GridCacheOffheapManager.GridCacheDataStore(newCtx, partId, true, defrgCtx.busyLock(), defrgCtx.log);

        newCacheDataStore.init();

        PageMemory memory = mappingRegion.pageMemory();

        FullPageId linkMapMetaPageId = new FullPageId(memory.allocatePage(grpId, partId, FLAG_DATA), grpId);

        LinkMap m = new LinkMap(grpCtx, memory, linkMapMetaPageId.pageId(), true);

        Iterable<CacheDataStore> stores = grpCtx.offheap().cacheDataStores();

        CacheDataStore oldCacheDataStore = StreamSupport
            .stream(stores.spliterator(), false)
            .filter(s -> grpId == s.tree().groupId())
            .filter(s -> partId == s.partId())
            .findFirst()
            .orElse(null);

        CacheDataTree tree = oldCacheDataStore.tree();

        CacheDataTree newTree = newCacheDataStore.tree();
        PendingEntriesTree newPendingTree = newCacheDataStore.pendingTree();
        AbstractFreeList<CacheDataRow> freeList = newCacheDataStore.getCacheStoreFreeList();

        treeIterator.iterate(tree, cachePageMem, (tree0, io, pageAddr, idx) -> {
            AbstractDataLeafIO leafIo = (AbstractDataLeafIO)io;
            CacheDataRow row = tree.getRow(io, pageAddr, idx);

            int cacheId = row.cacheId();

            // Reuse row that we just read.
            row.link(0);

            // "insertDataRow" will corrupt page memory if we don't do this.
            if (row instanceof DataRow && !grpCtx.storeCacheIdInDataPage())
                ((DataRow)row).cacheId(CU.UNDEFINED_CACHE_ID);

            freeList.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);

            // Put it back.
            if (row instanceof DataRow)
                ((DataRow)row).cacheId(cacheId);

            newTree.putx(row);

            long newLink = row.link();

            m.put(leafIo.getLink(pageAddr, idx), newLink);

            if (row.expireTime() != 0)
                newPendingTree.putx(new PendingRow(cacheId, row.expireTime(), newLink));

            return true;
        });

        freeList.saveMetadata(IoStatisticsHolderNoOp.INSTANCE);

        copyCacheMetadata(
            cachePageMem,
            oldCacheDataStore,
            partPageMem,
            newCacheDataStore,
            grpId,
            partId,
            pageSize
        );

        //TODO Invalidate mapping in mapping region?
        //TODO Invalidate PageStore for this partition.
        return m;
    }

    /** */
    private void copyCacheMetadata(
        PageMemoryEx oldPageMemory,
        CacheDataStore oldCacheDataStore,
        PageMemoryEx newPageMemory,
        CacheDataStore newCacheDataStore,
        int grpId,
        int partId,
        int pageSize
    ) throws IgniteCheckedException {
        long partMetaPageId = oldPageMemory.partitionMetaPageId(grpId, partId); // Same for all page memories.

        access(ACCESS_READ, oldPageMemory, grpId, partMetaPageId, oldPartMetaPageAddr -> {
            PagePartitionMetaIO oldPartMetaIo = PageIO.getPageIO(oldPartMetaPageAddr);

            // Newer meta versions may contain new data that we don't copy during defragmentation.
            assert Arrays.asList(1, 2).contains(oldPartMetaIo.getVersion()) : oldPartMetaIo.getVersion();

            access(ACCESS_WRITE, newPageMemory, grpId, partMetaPageId, newPartMetaPageAddr -> {
                PagePartitionMetaIOV2 newPartMetaIo = PageIO.getPageIO(newPartMetaPageAddr);

                // Copy partition state.
                byte partState = oldPartMetaIo.getPartitionState(oldPartMetaPageAddr);
                newPartMetaIo.setPartitionState(newPartMetaPageAddr, partState);

                // Copy cache size for single cache group.
                long size = oldPartMetaIo.getSize(oldPartMetaPageAddr);
                newPartMetaIo.setSize(newPartMetaPageAddr, size);

                // Copy update counter value.
                long updateCntr = oldPartMetaIo.getUpdateCounter(oldPartMetaPageAddr);
                newPartMetaIo.setUpdateCounter(newPartMetaPageAddr, updateCntr);

                // Copy global remove Id.
                long rmvId = oldPartMetaIo.getGlobalRemoveId(oldPartMetaPageAddr);
                newPartMetaIo.setGlobalRemoveId(newPartMetaPageAddr, rmvId);

                // Copy cache sizes for shared cache group.
                long oldCountersPageId = oldPartMetaIo.getCountersPageId(oldPartMetaPageAddr);
                if (oldCountersPageId != 0L) {
                    //TODO Extract method or something. This code block is too big.
                    long newCountersPageId = newPageMemory.allocatePage(grpId, partId, FLAG_DATA);

                    newPartMetaIo.setCountersPageId(newPartMetaPageAddr, newCountersPageId);

                    AtomicLong nextNewCountersPageIdRef = new AtomicLong(newCountersPageId);
                    AtomicLong nextOldCountersPageIdRef = new AtomicLong(oldCountersPageId);

                    while (nextNewCountersPageIdRef.get() != 0L) {
                        access(ACCESS_READ, oldPageMemory, grpId, nextOldCountersPageIdRef.get(), oldCountersPageAddr ->
                            access(ACCESS_WRITE, newPageMemory, grpId, nextNewCountersPageIdRef.get(), newCountersPageAddr -> {
                                PagePartitionCountersIO newPartCountersIo = PagePartitionCountersIO.VERSIONS.latest();

                                newPartCountersIo.initNewPage(newCountersPageAddr, nextNewCountersPageIdRef.get(), pageSize);

                                PagePartitionCountersIO oldCountersPageIo = PageIO.getPageIO(oldCountersPageAddr);

                                oldCountersPageIo.copyCacheSizes(
                                    oldCountersPageAddr,
                                    newCountersPageAddr
                                );

                                if (oldCountersPageIo.getLastFlag(oldCountersPageAddr)) {
                                    newPartCountersIo.setLastFlag(newCountersPageAddr, true);

                                    nextOldCountersPageIdRef.set(0L);
                                    nextNewCountersPageIdRef.set(0L);
                                }
                                else {
                                    nextOldCountersPageIdRef.set(oldCountersPageIo.getNextCountersPageId(oldCountersPageAddr));

                                    long nextNewCountersPageId = newPageMemory.allocatePage(grpId, partId, FLAG_DATA);

                                    newPartCountersIo.setNextCountersPageId(newCountersPageAddr, nextNewCountersPageId);

                                    nextNewCountersPageIdRef.set(nextNewCountersPageId);
                                }

                                return null;
                            })
                        );
                    }
                }

                // Copy counter gaps.
                long oldGapsLink = oldPartMetaIo.getGapsLink(oldPartMetaPageAddr);
                if (oldGapsLink != 0L) {
                    byte[] gapsBytes = oldCacheDataStore.partStorage().readRow(oldGapsLink);

                    SimpleDataRow gapsDataRow = new SimpleDataRow(partId, gapsBytes);

                    newCacheDataStore.partStorage().insertDataRow(gapsDataRow, IoStatisticsHolderNoOp.INSTANCE);

                    newPartMetaIo.setGapsLink(newPartMetaPageAddr, gapsDataRow.link());

                    newCacheDataStore.partStorage().saveMetadata(IoStatisticsHolderNoOp.INSTANCE);
                }

                return null;
            });

            return null;
        });
    }

    /**
     * Defragmentate indexing partition.
     *
     * @param grpCtx
     * @param mappingByPartition
     *
     * @throws IgniteCheckedException If failed.
     */
    private void defragmentIndexPartition(
        CacheGroupContext grpCtx,
        Map<Integer, LinkMap> mappingByPartition
    ) throws IgniteCheckedException {
        GridQueryProcessor query = grpCtx.caches().get(0).kernalContext().query();

        if (!query.moduleEnabled())
            return;

        final GridQueryIndexing idx = query.getIndexing();

        idx.defragmentator().defragmentate(grpCtx, defrgCtx, mappingByPartition, log);
    }

}
