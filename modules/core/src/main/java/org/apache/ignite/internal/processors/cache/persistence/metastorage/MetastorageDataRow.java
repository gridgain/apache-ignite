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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 *
 */
public class MetastorageDataRow extends SimpleDataRow implements MetastorageSearchRow {
    /** */
    private String key;

    /** */
    public MetastorageDataRow(long link, String key, byte[] val) {
        // TODO FIXME use static setter to recalc part */
        super(link, MetaStorage.PRESERVE_LEGACY_METASTORAGE_PARTITION_ID ?
            PageIdAllocator.OLD_METASTORE_PARTITION: PageIdAllocator.METASTORE_PARTITION, val);
        this.key = key;
    }

    /** */
    public MetastorageDataRow(String key, byte[] val) {
        this(0, key, val);
    }

    /**
     * @return Key.
     */
    public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public IOVersions<? extends AbstractDataPageIO> ioVersions() {
        return MetastoreDataPageIO.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "key=" + key;
    }
}
