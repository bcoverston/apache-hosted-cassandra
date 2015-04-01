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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractSSTableSimpleWriter implements Closeable
{
    protected final File directory;
    protected final CFMetaData metadata;
    protected DecoratedKey currentKey;
    protected PartitionUpdate update;
    protected ByteBuffer currentSuperColumn;
    protected final CounterId counterid = CounterId.generate();
    private SSTableFormat.Type formatType = DatabaseDescriptor.getSSTableFormat();
    protected static AtomicInteger generation = new AtomicInteger(0);


    public AbstractSSTableSimpleWriter(File directory, CFMetaData metadata, IPartitioner partitioner)
    {
        this.metadata = metadata;
        this.directory = directory;
        DatabaseDescriptor.setPartitioner(partitioner);
    }

    protected void setSSTableFormatType(SSTableFormat.Type type)
    {
        this.formatType = type;
    }

    protected SSTableWriter getWriter()
    {
        return SSTableWriter.create(createDescriptor(directory, metadata.ksName, metadata.cfName, formatType),
                                    0,
                                    ActiveRepairService.UNREPAIRED_SSTABLE,
                                    new SerializationHeader(metadata, metadata.partitionColumns(), AtomStats.NO_STATS, true));
    }

    protected static Descriptor createDescriptor(File directory, final String keyspace, final String columnFamily, final SSTableFormat.Type fmt)
    {
        int maxGen = getNextGeneration(directory, columnFamily);
        return new Descriptor(directory, keyspace, columnFamily, maxGen + 1, Descriptor.Type.TEMP, fmt);
    }

    private static int getNextGeneration(File directory, final String columnFamily)
    {
        final Set<Descriptor> existing = new HashSet<>();
        directory.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (desc == null)
                    return false;

                if (desc.cfname.equals(columnFamily))
                    existing.add(desc);

                return false;
            }
        });
        int maxGen = generation.getAndIncrement();
        for (Descriptor desc : existing)
        {
            while (desc.generation > maxGen)
            {
                maxGen = generation.getAndIncrement();
            }
        }
        return maxGen;
    }

    /**
     * Start a new row whose key is {@code key}.
     * @param key the row key
     */
    public void newRow(ByteBuffer key) throws IOException
    {
        if (currentKey != null && !update.isEmpty())
            writePartition(currentKey, update);

        currentKey = DatabaseDescriptor.getPartitioner().decorateKey(key);
        update = getPartitionUpdate();
    }

    /**
     * Start a new super column with name {@code name}.
     * @param name the name for the super column
     */
    public void newSuperColumn(ByteBuffer name)
    {
        if (!update.metadata().isSuper())
            throw new IllegalStateException("Cannot add a super column to a standard table");

        currentSuperColumn = name;
    }

    protected void addColumn(ByteBuffer name, Cell cell) throws IOException
    {
        // TODO: we would need to extract from name the actual column name and clustering
        // maybe it's a good time to remove the simple writer in favor of just the
        // CQLSStableWriter?
        throw new UnsupportedOperationException();

        //if (update.metadata().isSuper())
        //{
        //    if (currentSuperColumn == null)
        //        throw new IllegalStateException("Trying to add a cell to a super column family, but no super cell has been started.");

        //    cell = cell.withUpdatedName(columnFamily.getComparator().makeCellName(currentSuperColumn, cell.name().toByteBuffer()));
        //}

        //update.newRowUpdate(clustering).addCell(column, cell);
    }

    /**
     * Insert a new "regular" column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the column value
     * @param timestamp the column timestamp
     */
    public void addColumn(ByteBuffer name, ByteBuffer value, long timestamp) throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //addColumn(name, Cells.create(value, timestamp, 0, null));
    }

    /**
     * Insert a new expiring column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the column value
     * @param timestamp the column timestamp
     * @param ttl the column time to live in seconds
     * @param expirationTimestampMS the local expiration timestamp in milliseconds. This is the server time timestamp used for actually
     * expiring the column, and as a consequence should be synchronized with the cassandra servers time. If {@code timestamp} represents
     * the insertion time in microseconds (which is not required), this should be {@code (timestamp / 1000) + (ttl * 1000)}.
     */
    public void addExpiringColumn(ByteBuffer name, ByteBuffer value, long timestamp, int ttl, long expirationTimestampMS) throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //addColumn(name, Cells.create(value, timestamp, ttl, (int)(expirationTimestampMS / 1000), null));
    }

    /**
     * Insert a new counter column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the value of the counter
     */
    public void addCounterColumn(ByteBuffer name, long value) throws IOException
    {
        // TODO
        throw new UnsupportedOperationException();
        //addColumn(name, Cells.createCounter(CounterContext.instance().createGlobal(counterid, 1L, value), System.currentTimeMillis()));
    }

    /**
     * Package protected for use by AbstractCQLSSTableWriter.
     * Not meant to be exposed publicly.
     */
    PartitionUpdate currentUpdate()
    {
        return update;
    }

    /**
     * Package protected for use by AbstractCQLSSTableWriter.
     * Not meant to be exposed publicly.
     */
    DecoratedKey currentKey()
    {
        return currentKey;
    }

    protected abstract void writePartition(DecoratedKey key, PartitionUpdate update) throws IOException;

    protected abstract PartitionUpdate getPartitionUpdate() throws IOException;
}
