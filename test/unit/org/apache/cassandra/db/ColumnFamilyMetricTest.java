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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ColumnFamilyMetricTest
{
    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("Keyspace1",
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD("Keyspace1", "Standard2"));
    }

    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");
        cfs.disableAutoCompaction();

        cfs.truncateBlocking();

        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(cfs.metadata, FBUtilities.timestampMicros(), String.valueOf(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        }
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstables = cfs.getSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(size, cfs.metric.totalDiskSpaceUsed.getCount());

        cfs.truncateBlocking();

        // after truncate, size metrics should be down to 0
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());

        cfs.enableAutoCompaction();
    }

}
