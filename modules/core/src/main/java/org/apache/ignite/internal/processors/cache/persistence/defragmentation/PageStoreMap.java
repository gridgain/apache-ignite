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

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.PageStoreCollection;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;

/** */
class PageStoreMap implements PageStoreCollection {
    /** GroupId -> PartId -> PageStore */
    private final IntMap<IntMap<PageStore>> grpPageStoresMap = new IntRWHashMap<>();

    /** */
    public void addPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        IntMap<PageStore> pageStoresMap = grpPageStoresMap.get(grpId);

        if (pageStoresMap == null)
            grpPageStoresMap.put(grpId, pageStoresMap = new IntRWHashMap<>());

        pageStoresMap.put(partId, pageStore);
    }

    /** {@inheritDoc} */
    @Override public PageStore getStore(int grpId, int partId) {
        IntMap<PageStore> partPageStoresMap = grpPageStoresMap.get(grpId);

        assert partPageStoresMap != null; //TODO Throw meaningful exception?

        PageStore pageStore = partPageStoresMap.get(partId);

        assert pageStore != null; //TODO Throw meaningful exception?

        return pageStore;
    }

    /** {@inheritDoc} */
    @Override public Collection<PageStore> getStores(int grpId) {
        IntMap<PageStore> partPageStoresMap = grpPageStoresMap.get(grpId);

        assert partPageStoresMap != null; //TODO Throw meaningful exception?

        return Arrays.asList(partPageStoresMap.values());
    }
}
