/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableId;


public class CommitLogReplayer extends AbstractCommitLogReplayer
{
    @VisibleForTesting
    protected CommitLogReader commitLogReader;

    CommitLogReplayer(CommitLog commitLog)
    {
        super(commitLog);
        commitLogReader = new CommitLogReader(); 
    }

    CommitLogReplayer(CommitLog commitLog,
                      CommitLogPosition globalPosition,
                      Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted,
                      ReplayFilter replayFilter)
    {
        super(commitLog, globalPosition, cfPersisted, replayFilter);
        commitLogReader = new CommitLogReader(); 
    }

    public static CommitLogReplayer construct(CommitLog commitLog)
    {
        return new CommitLogReplayer(commitLog);
    }

    @Override
    public AbstractCommitLogReader getCommitLogReader()
    {
        return commitLogReader;
    }

    public void replayPath(File file, boolean tolerateTruncation) throws IOException
    {
        sawCDCMutation = false;
        commitLogReader.readCommitLogSegment(this, file, globalPosition, CommitLogReader.ALL_MUTATIONS, tolerateTruncation);
        if (sawCDCMutation)
            handleCDCReplayCompletion(file);
    }

    public void replayFiles(File[] clogs) throws IOException
    {
        List<File> filteredLogs = CommitLogReader.filterCommitLogFiles(clogs);
        int i = 0;
        for (File file: filteredLogs)
        {
            i++;
            sawCDCMutation = false;
            commitLogReader.readCommitLogSegment(this, file, globalPosition, i == filteredLogs.size());
            if (sawCDCMutation)
                handleCDCReplayCompletion(file);
        }
    }


    /**
     * Upon replay completion, CDC needs to hard-link files in the CDC folder and calculate index files so consumers can
     * begin their work.
     */
    private void handleCDCReplayCompletion(File f) throws IOException
    {
        // Can only reach this point if CDC is enabled, thus we have a CDCSegmentManager
        ((CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager).addCDCSize(f.length());

        File dest = new File(DatabaseDescriptor.getCDCLogLocation(), f.getName());

        // If hard link already exists, assume it's from a previous node run. If people are mucking around in the cdc_raw
        // directory that's on them.
        if (!dest.exists())
            FileUtils.createHardLink(f, dest);

        // The reader has already verified we can deserialize the descriptor.
        CommitLogDescriptor desc;
        try(RandomAccessReader reader = RandomAccessReader.open(f))
        {
            desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            assert desc != null;
            assert f.length() < Integer.MAX_VALUE;
            CommitLogSegment.writeCDCIndexFile(desc, (int)f.length(), true);
        }
    }
}
