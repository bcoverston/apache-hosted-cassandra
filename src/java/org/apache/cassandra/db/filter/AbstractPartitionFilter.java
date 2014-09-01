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
package org.apache.cassandra.db.filter;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;

public abstract class AbstractPartitionFilter implements PartitionFilter
{
    protected enum Kind
    {
        SLICE (SlicePartitionFilter.deserializer),
        NAMES (NamesPartitionFilter.deserializer);

        private final InternalDeserializer deserializer;

        private Kind(InternalDeserializer deserializer)
        {
            this.deserializer = deserializer;
        }
    }

    static final Serializer serializer = new FilterSerializer();

    private final Kind kind;
    protected final PartitionColumns queriedColumns;
    protected final boolean reversed;

    protected AbstractPartitionFilter(Kind kind, PartitionColumns queriedColumns, boolean reversed)
    {
        this.kind = kind;
        this.queriedColumns = queriedColumns;
        this.reversed = reversed;
    }

    public PartitionColumns queriedColumns()
    {
        return queriedColumns;
    }

    public boolean isReversed()
    {
        return reversed;
    }

    protected abstract void serializeInternal(DataOutputPlus out, int version) throws IOException;
    protected abstract long serializedSizeInternal(int version, TypeSizes sizes);

    private static class FilterSerializer implements Serializer
    {
        public void serialize(PartitionFilter pfilter, DataOutputPlus out, int version) throws IOException
        {
            AbstractPartitionFilter filter = (AbstractPartitionFilter)pfilter;

            out.writeByte(filter.kind.ordinal());
            Columns.serializer.serialize(filter.queriedColumns().statics, out);
            Columns.serializer.serialize(filter.queriedColumns().regulars, out);
            out.writeBoolean(filter.isReversed());

            filter.serializeInternal(out, version);
        }

        public PartitionFilter deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            Columns statics = Columns.serializer.deserialize(in, metadata);
            Columns regulars = Columns.serializer.deserialize(in, metadata);
            PartitionColumns columns = new PartitionColumns(statics, regulars);
            boolean reversed = in.readBoolean();

            return kind.deserializer.deserialize(in, version, metadata, columns, reversed);
        }

        public long serializedSize(PartitionFilter pfilter, int version)
        {
            AbstractPartitionFilter filter = (AbstractPartitionFilter)pfilter;

            TypeSizes sizes = TypeSizes.NATIVE;
            return 1
                 + Columns.serializer.serializedSize(filter.queriedColumns().statics, sizes)
                 + Columns.serializer.serializedSize(filter.queriedColumns().regulars, sizes)
                 + sizes.sizeof(filter.isReversed())
                 + filter.serializedSizeInternal(version, sizes);
        }
    }

    protected static abstract class InternalDeserializer
    {
        public abstract PartitionFilter deserialize(DataInput in, int version, CFMetaData metadata, PartitionColumns columns, boolean reversed) throws IOException;
    }
}

