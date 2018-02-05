/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadErrorReason;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadException;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JVMStabilityInspector;

public abstract class AbstractCommitLogReader
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogReader.class);

    private final Map<TableId, AtomicInteger> invalidMutations;

    public AbstractCommitLogReader()
    {
        invalidMutations = new HashMap<>();
    }

    public Set<Map.Entry<TableId, AtomicInteger>> getInvalidMutations()
    {
        return invalidMutations.entrySet();
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param handler Handler that will take action based on deserialized Mutations
     * @param inputBuffer raw byte array w/Mutation data
     * @param size deserialized size of mutation
     * @param minPosition We need to suppress replay of mutations that are before the required minPosition
     * @param entryLocation filePointer offset of end of mutation within CommitLogSegment
     * @param desc CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    protected void readMutation(CommitLogReadHandler handler,
                                byte[] inputBuffer,
                                int size,
                                CommitLogPosition minPosition,
                                final int entryLocation,
                                final CommitLogDescriptor desc) throws IOException
    {
        readMutation(handler, inputBuffer, 0, size, minPosition, entryLocation, desc);
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param handler Handler that will take action based on deserialized Mutations
     * @param inputBuffer raw byte array w/Mutation data
     * @param offset offset in intputBuffer
     * @param size deserialized size of mutation
     * @param minPosition We need to suppress replay of mutations that are before the required minPosition
     * @param entryLocation filePointer offset of end of mutation within CommitLogSegment
     * @param desc CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    protected void readMutation(CommitLogReadHandler handler,
                                byte[] inputBuffer,
                                int offset,
                                int size,
                                CommitLogPosition minPosition,
                                final int entryLocation,
                                final CommitLogDescriptor desc) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment.
        boolean shouldReplay = entryLocation > minPosition.position;

        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, offset, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       desc.getMessagingVersion(),
                                                       SerializationHelper.Flag.LOCAL);
            // doublecheck that what we read is still] valid for the current schema
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
                upd.validate();
        }
        catch (UnknownTableException ex)
        {
            if (ex.id == null)
                return;
            AtomicInteger i = invalidMutations.get(ex.id);
            if (i == null)
            {
                i = new AtomicInteger(1);
                invalidMutations.put(ex.id, i);
            }
            else
                i.incrementAndGet();
            return;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            Path p = Files.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(p)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible.
            handler.handleUnrecoverableError(new CommitLogReadException(
                String.format(
                    "Unexpected error deserializing mutation; saved to %s.  " +
                    "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
                    "Exception follows: %s", p.toString(), t),
                CommitLogReadErrorReason.MUTATION_ERROR,
                false));
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("Read mutation for {}.{}: {}", mutation.getKeyspaceName(), mutation.key(),
                         "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");

        if (shouldReplay)
            handler.handleMutation(mutation, size, entryLocation, desc);
    }
}
