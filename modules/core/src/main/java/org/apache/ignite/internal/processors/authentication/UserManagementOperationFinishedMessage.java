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

package org.apache.ignite.internal.processors.authentication;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message indicating that snapshot has been finished on a single node.
 */
public class UserManagementOperationFinishedMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Id. */
    private IgniteUuid opId;

    /** ACK phase flag. */
    private boolean ackPhase;

    /** Error message. */
    private String errorMsg;

    /** */
    public UserManagementOperationFinishedMessage() {
    }

    /**
     * @param opId operation id
     * @param ackPhase Ack phase flag.
     * @param errorMsg error message
     */
    public UserManagementOperationFinishedMessage(IgniteUuid opId, boolean ackPhase, String errorMsg) {
        this.opId = opId;
        this.ackPhase = ackPhase;
        this.errorMsg = errorMsg;
    }

    /**
     * @return Operation ID,
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @return ACK phase flag.
     */
    public boolean ackPhase() {
        return ackPhase;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return errorMsg == null;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errorMsg;
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean("ackPhase", ackPhase))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("errorMsg", errorMsg))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIgniteUuid("opId", opId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                ackPhase = reader.readBoolean("ackPhase");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errorMsg = reader.readString("errorMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                opId = reader.readIgniteUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(UserManagementOperationFinishedMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 2100;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserManagementOperationFinishedMessage.class, this);
    }
}
