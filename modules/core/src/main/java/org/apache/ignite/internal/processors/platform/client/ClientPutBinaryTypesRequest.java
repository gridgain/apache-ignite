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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import java.util.Collection;

/**
 * Binary types update request.
 */
class ClientPutBinaryTypesRequest extends ClientRequest {
    /** Metas. */
    private final Collection<BinaryMetadata> metas;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    ClientPutBinaryTypesRequest(BinaryRawReaderEx reader) {
        super(reader);

        metas = PlatformUtils.readBinaryMetadata(reader);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClientResponse process(GridKernalContext ctx) {
        BinaryContext binCtx = ((CacheObjectBinaryProcessorImpl) ctx.cacheObjects()).binaryContext();

        for (BinaryMetadata meta : metas)
            binCtx.updateMetadata(meta.typeId(), meta);

        return super.process(ctx);
    }
}
