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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;

public class StaticRow extends AbstractReusableRow
{
    private final DeletionTime deletion;
    private final RowDataBlock data;

    private final long maxLiveTimestamp;
    private final int nowInSec;

    private StaticRow(long maxLiveTimestamp, DeletionTime deletion, RowDataBlock data, int nowInSec)
    {
        this.maxLiveTimestamp = maxLiveTimestamp;
        this.deletion = deletion;
        this.data = data;
        this.nowInSec = nowInSec;
    }

    protected RowDataBlock data()
    {
        return data;
    }

    protected int row()
    {
        return 0;
    }

    public Clustering clustering()
    {
        return Clustering.STATIC_CLUSTERING;
    }

    public LivenessInfo partitionKeyLivenessInfo()
    {
        return LivenessInfo.NONE;
    }

    public long maxLiveTimestamp()
    {
        return maxLiveTimestamp;
    }

    public DeletionTime deletion()
    {
        return deletion;
    }

    public int nowInSec()
    {
        return nowInSec;
    }

    public static Builder builder(Columns columns, int nowInSec)
    {
        return new Builder(columns, nowInSec);
    }

    public static class Builder extends RowDataBlock.Writer
    {
        private final RowDataBlock data;
        private DeletionTime deletion = DeletionTime.LIVE;
        private long maxLiveTimestamp;
        private final int nowInSec;

        public Builder(Columns columns, int nowInSec)
        {
            this.data = new RowDataBlock(columns, 1, false);
            this.nowInSec = nowInSec;
            updateWriter(data);
        }

        public void writeClusteringValue(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException();
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            // Static rows are special and don't really have an existence unless they have live cells,
            // so we shouldn't have any partition key liveness info.
            assert info.equals(LivenessInfo.NONE);
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        public void writeMaxLiveTimestamp(long maxLiveTimestamp)
        {
            this.maxLiveTimestamp = maxLiveTimestamp;
        }

        public StaticRow build()
        {
            return new StaticRow(maxLiveTimestamp, deletion, data, nowInSec);
        }
    }
}
