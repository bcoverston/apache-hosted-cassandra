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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SerializationHelper
{
    /**
     * Flag affecting deserialization behavior.
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public static enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE;
    }

    private final Flag flag;
    public final int nowInSec;
    public final int version;

    private long maxLiveTimestamp;
    private final ReusableLivenessInfo livenessInfo = new ReusableLivenessInfo();

    // The currently read row liveness infos (timestamp, ttl and localDeletionTime).
    private long rowTimestamp;
    private int rowTTL;
    private int rowLocalDeletionTime;

    public SerializationHelper(int version, Flag flag, int nowInSec)
    {
        this.flag = flag;
        this.nowInSec = nowInSec;
        this.version = version;
    }

    public void writePartitionKeyLivenessInfo(Row.Writer writer, long timestamp, int ttl, int localDeletionTime)
    {
        livenessInfo.setTo(timestamp, ttl, localDeletionTime);
        maxLiveTimestamp = timestamp;
        writer.writePartitionKeyLivenessInfo(livenessInfo);

        rowTimestamp = timestamp;
        rowTTL = ttl;
        rowLocalDeletionTime = localDeletionTime;
    }

    public long getMaxLiveTimestamp()
    {
        return maxLiveTimestamp;
    }

    public long getRowTimestamp()
    {
        return rowTimestamp;
    }

    public int getRowTTL()
    {
        return rowTTL;
    }

    public int getRowLocalDeletionTime()
    {
        return rowLocalDeletionTime;
    }

    private boolean canExpire(LivenessInfo info)
    {
        return info.hasTTL()
            && flag != Flag.PRESERVE_SIZE
            && !info.isLive(nowInSec);
    }

    public void writeCell(Row.Writer writer,
                          ColumnDefinition column,
                          boolean isCounter,
                          ByteBuffer value,
                          long timestamp,
                          int localDelTime,
                          int ttl,
                          CellPath path)
    {
        livenessInfo.setTo(timestamp, ttl, localDelTime);

        if (isCounter)
        {
            if (flag == Flag.FROM_REMOTE || (flag == Flag.LOCAL && Cells.counterContextManager.shouldClearLocal(value)))
                value = Cells.counterContextManager.clearAllLocal(value);
        }
        else
        {
            if (canExpire(livenessInfo))
            {
                // The column is now expired, we can safely return a simple tombstone. Note that as long as the expiring
                // column and the tombstone put together live longer than GC grace seconds, we'll fulfil our responsibility
                // to repair. See discussion at
                // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
                livenessInfo.setTo(timestamp, LivenessInfo.NO_TTL, localDelTime - ttl);
                value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            }
        }

        // Collect max live timestamp
        if (livenessInfo.isLive(nowInSec) && timestamp > maxLiveTimestamp)
            maxLiveTimestamp = timestamp;

        writer.writeCell(column, isCounter, value, livenessInfo, path);
    }

    public void updateForSkippedCell(long timestamp, int localDelTime)
    {
        // Collect max live timestamp
        if (localDelTime >= nowInSec && timestamp > maxLiveTimestamp)
            maxLiveTimestamp = timestamp;
    }
}
