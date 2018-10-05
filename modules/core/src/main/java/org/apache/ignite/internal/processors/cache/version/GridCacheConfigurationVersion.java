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
package org.apache.ignite.internal.processors.cache.version;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

public class GridCacheConfigurationVersion implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private volatile int id;

    private volatile GridCacheConfigurationChangeAction lastAction;

    private final String cacheName;

    private final String cacheGroupName;

    private final boolean staticlyConfigured;

    public GridCacheConfigurationVersion(String cacheName, String cacheGroupName, boolean staticlyConfigured) {
        this(0, null, cacheName, cacheGroupName, staticlyConfigured);
    }

    private GridCacheConfigurationVersion(
        int id,
        GridCacheConfigurationChangeAction action,
        String cacheName,
        String cacheGroupName,
        boolean staticlyConfigured
    ) {
        this.id = id;
        this.lastAction = action;
        this.cacheName = cacheName;
        this.cacheGroupName = cacheGroupName;
        this.staticlyConfigured = staticlyConfigured;
    }

    public int id() {
        return id;
    }

    public GridCacheConfigurationChangeAction lastAction() {
        return lastAction;
    }

    public void updateVersion(@NotNull GridCacheConfigurationChangeAction action) {
        synchronized (this){
            if(isNeedUpdateVersion(action)){
                this.id = id + 1;
                this.lastAction = action;
            }
        }
    }

    public boolean isNeedUpdateVersion(@NotNull GridCacheConfigurationChangeAction action){
        if(staticlyConfigured)
            return false;

        if(action == GridCacheConfigurationChangeAction.META_CHANGED)
            return true;

        return lastAction != action;
    }

    public String cacheGroupName(){ return cacheGroupName; }

    public String cacheName(){ return cacheName; }

    public boolean staticlyConfigured(){ return staticlyConfigured; }

     /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConfigurationVersion.class, this);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GridCacheConfigurationVersion version = (GridCacheConfigurationVersion)o;
        return id == version.id &&
            staticlyConfigured == version.staticlyConfigured &&
            lastAction == version.lastAction &&
            Objects.equals(cacheName, version.cacheName) &&
            Objects.equals(cacheGroupName, version.cacheGroupName);
    }

    @Override public int hashCode() {
        return Objects.hash(cacheName, cacheGroupName, staticlyConfigured);
    }
}