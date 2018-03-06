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

package org.apache.ignite.internal.thinclient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Fields query pager.
 */
class ClientFieldsQueryPager extends GenericQueryPager<List<?>> implements FieldsQueryPager<List<?>> {
    /** Keep binary. */
    private final boolean keepBinary;

    /** Field names. */
    private List<String> fieldNames = new ArrayList<>();

    /** Constructor. */
    ClientFieldsQueryPager(
        ReliableChannel ch,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<BinaryOutputStream> qryWriter,
        boolean keepBinary
    ) {
        super(ch, qryOp, pageQryOp, qryWriter);

        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override Collection<List<?>> readEntries(BinaryInputStream in) {
        if (!hasFirstPage())
            fieldNames = new ArrayList<>(SerDes.read(in, ignored -> (String)SerDes.readObject(in, keepBinary)));

        int rowCnt = in.readInt();

        Collection<List<?>> res = new ArrayList<>(rowCnt);

        for (int r = 0; r < rowCnt; r++) {
            List<?> row = new ArrayList<>(fieldNames.size());

            for (int f = 0; f < fieldNames.size(); f++)
                row.add(SerDes.readObject(in, keepBinary));

            res.add(row);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<String> getFieldNames() {
        return fieldNames;
    }
}
