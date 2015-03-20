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
package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.utils.FBUtilities;

// TODO: find a better name
public class ReadPartition extends ArrayBackedPartition implements Iterable<Row>
{
    private ReadPartition(CFMetaData metadata,
                          DecoratedKey partitionKey,
                          PartitionColumns columns,
                          int initialRowCapacity,
                          boolean sortable,
                          int nowInSec)
    {
        super(metadata, partitionKey, DeletionTime.LIVE, columns, initialRowCapacity, sortable, nowInSec);
    }

    /**
     * Create a ReadPartition holding all the rows of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     */
    public static ReadPartition create(RowIterator iterator)
    {
        ReadPartition partition = new ReadPartition(iterator.metadata(),
                                                    iterator.partitionKey(),
                                                    iterator.columns(),
                                                    4,
                                                    iterator.isReverseOrder(),
                                                    iterator.nowInSec());

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer(iterator.nowInSec());

        while (iterator.hasNext())
            iterator.next().copyTo(writer);

        // A Partition (or more precisely AbstractPartitionData) always assumes that its data is in clustering
        // order. So if we've just added them in reverse clustering order, reverse them.
        if (iterator.isReverseOrder())
            partition.reverse();

        return partition;
    }

    public Iterator<Row> iterator()
    {
        return iterator(createdAtInSec);
    }

    public RowIterator rowIterator()
    {
        final Iterator<Row> iter = iterator();
        return new RowIterator()
        {
            public CFMetaData metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public PartitionColumns columns()
            {
                return columns;
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public Row staticRow()
            {
                return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            }

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next()
            {
                return iter.next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public int nowInSec()
            {
                return createdAtInSec;
            }

            public void close()
            {
            }
        };
    }

    public Row getRow(Clustering clustering)
    {
        Row row = searchIterator(columns(), false, createdAtInSec).next(clustering);
        // Note that for statics, this will never return null, this will return an empty row. However,
        // it's more consistent for this method to return null if we don't really have a static row.
        return row == null || (clustering == Clustering.STATIC_CLUSTERING && row.isEmpty()) ? null : row;
    }

    @Override
    public String toString()
    {
        RowIterator iterator = rowIterator();
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = iterator.metadata();
        PartitionColumns columns = iterator.columns();

        sb.append(String.format("[%s.%s] key=%s columns=%s reversed=%b",
                                metadata.ksName,
                                metadata.cfName,
                                metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                columns,
                                iterator.isReverseOrder()));

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(iterator.staticRow().toString(metadata));

        while (iterator.hasNext())
            sb.append("\n    ").append(iterator.next().toString(metadata));

        return sb.toString();
    }
}
