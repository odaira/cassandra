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

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableId;

public interface ICommitLog
{
    CommitLogPosition getCurrentPosition();

    CommitLogPosition add(Mutation mutation);

    int recover() throws IOException;

    void shutdownBlocking() throws InterruptedException;

    void discardCompletedSegments(TableId id, CommitLogPosition commitLogLowerBound, CommitLogPosition commitLogUpperBound);

    void forceRecycleAllSegments(Iterable<TableId> droppedCfs);

    void forceRecycleAllSegments();

    int resetUnsafe(boolean deleteSegments) throws IOException;

    void stopUnsafe(boolean deleteSegments) throws IOException;

    int restartUnsafe() throws IOException;
}
