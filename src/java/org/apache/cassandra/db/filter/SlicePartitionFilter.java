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
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.columniterator.SSTableIterator;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * A filter over a single partition.
 */
public class SlicePartitionFilter extends AbstractPartitionFilter
{
    static final InternalDeserializer deserializer = new SliceDeserializer();

    private final Slices slices;

    public SlicePartitionFilter(PartitionColumns columns, Slices slices, boolean reversed)
    {
        super(Kind.SLICE, columns, reversed);
        this.slices = slices;
    }

    public Slices requestedSlices()
    {
        return slices;
    }

    public boolean selectsAllPartition()
    {
        return slices.size() == 1 && !slices.hasLowerBound() && !slices.hasUpperBound();
    }

    public boolean selects(Clustering clustering)
    {
        return slices.selects(clustering);
    }

    public SlicePartitionFilter forPaging(ClusteringComparator comparator, Clustering lastReturned)
    {
        Slices newSlices = slices.forPaging(comparator, lastReturned, reversed);
        return slices == newSlices
             ? this
             : new SlicePartitionFilter(queriedColumns, newSlices, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        // Partition is guaranteed to cover the whole filter if it includes the filter start and finish bounds.

        // (note that since partition is the head of a partition, to have no lower bound is ok)
        if (!slices.hasUpperBound() || partition.isEmpty())
            return false;

        return partition.metadata().comparator.compare(slices.get(slices.size() - 1).end(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return !reversed && slices.size() == 1 && !slices.hasLowerBound();
    }

    // Given another iterator, only return the atoms that match this filter
    public AtomIterator filter(AtomIterator iterator)
    {
        final Slices.InOrderTester tester = slices.inOrderTester(reversed);

        // Note that we don't filter markers because that's a bit trickier (we don't know in advance until when
        // the range extend) and it's harmless to left them.
        return new RowFilteringAtomIterator(iterator, FilteringRow.columnsFilteringRow(queriedColumns))
        {
            @Override
            protected boolean includeRow(Row row)
            {
                return tester.includes(row.clustering());
            }

            @Override
            public boolean hasNext()
            {
                return !tester.isDone() && super.hasNext();
            }
        };
    }

    public AtomIterator filter(SliceableAtomIterator iterator)
    {
        return slices.makeSliceIterator(iterator);
    }

    public AtomIterator getAtomIterator(Partition partition, int nowInSec)
    {
        return partition.atomIterator(queriedColumns, slices, reversed, nowInSec);
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        List<ByteBuffer> minClusteringValues = sstable.getSSTableMetadata().minClusteringValues;
        List<ByteBuffer> maxClusteringValues = sstable.getSSTableMetadata().maxClusteringValues;

        if (minClusteringValues.isEmpty() || maxClusteringValues.isEmpty())
            return true;

        return slices.intersects(minClusteringValues, maxClusteringValues);
    }

    public String toString(CFMetaData metadata)
    {
        return String.format("slice(%s, slices=%s, reversed=%b)", queriedColumns, slices, reversed);
    }

    public String toCQLString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();

        if (!selectsAllPartition())
            sb.append(slices.toCQLString(metadata));

        if (reversed)
        {
            sb.append(" ORDER BY (");
            int i = 0;
            for (ColumnDefinition column : metadata.clusteringColumns())
                sb.append(i++ == 0 ? "" : ", ").append(column.name).append(column.type instanceof ReversedType ? " ASC" : " DESC");
            sb.append(")");
        }
        return sb.toString();
    }

    protected void serializeInternal(DataOutputPlus out, int version) throws IOException
    {
        Slices.serializer.serialize(slices, out, version);
    }

    protected long serializedSizeInternal(int version, TypeSizes sizes)
    {
        return Slices.serializer.serializedSize(slices, version, sizes);
    }

    private static class SliceDeserializer extends InternalDeserializer
    {
        public PartitionFilter deserialize(DataInput in, int version, CFMetaData metadata, PartitionColumns columns, boolean reversed) throws IOException
        {
            Slices slices = Slices.serializer.deserialize(in, version, metadata);
            return new SlicePartitionFilter(columns, slices, reversed);
        }
    }

    // From SliceQueryFilter
    //public static class Serializer implements IVersionedSerializer<SliceQueryFilter>
    //{
    //    private CType type;

    //    public Serializer(CType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serialize(SliceQueryFilter f, DataOutputPlus out, int version) throws IOException
    //    {
    //        out.writeInt(f.slices.length);
    //        for (ColumnSlice slice : f.slices)
    //            type.sliceSerializer().serialize(slice, out, version);
    //        out.writeBoolean(f.reversed);
    //        int count = f.count;
    //        out.writeInt(count);

    //        out.writeInt(f.compositesToGroup);
    //    }

    //    public SliceQueryFilter deserialize(DataInput in, int version) throws IOException
    //    {
    //        ColumnSlice[] slices;
    //        slices = new ColumnSlice[in.readInt()];
    //        for (int i = 0; i < slices.length; i++)
    //            slices[i] = type.sliceSerializer().deserialize(in, version);
    //        boolean reversed = in.readBoolean();
    //        int count = in.readInt();
    //        int compositesToGroup = -1;
    //        compositesToGroup = in.readInt();

    //        return new SliceQueryFilter(slices, reversed, count, compositesToGroup);
    //    }

    //    public long serializedSize(SliceQueryFilter f, int version)
    //    {
    //        TypeSizes sizes = TypeSizes.NATIVE;

    //        int size = 0;
    //        size += sizes.sizeof(f.slices.length);
    //        for (ColumnSlice slice : f.slices)
    //            size += type.sliceSerializer().serializedSize(slice, version);
    //        size += sizes.sizeof(f.reversed);
    //        size += sizes.sizeof(f.count);

    //        size += sizes.sizeof(f.compositesToGroup);
    //        return size;
    //    }
    //}

    // From IDiskAtomFilter
    //public static class Serializer implements IVersionedSerializer<IDiskAtomFilter>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serialize(IDiskAtomFilter filter, DataOutputPlus out, int version) throws IOException
    //    {
    //        if (filter instanceof SliceQueryFilter)
    //        {
    //            out.writeByte(0);
    //            type.sliceQueryFilterSerializer().serialize((SliceQueryFilter)filter, out, version);
    //        }
    //        else
    //        {
    //            out.writeByte(1);
    //            type.namesQueryFilterSerializer().serialize((NamesQueryFilter)filter, out, version);
    //        }
    //    }

    //    public IDiskAtomFilter deserialize(DataInput in, int version) throws IOException
    //    {
    //        int b = in.readByte();
    //        if (b == 0)
    //        {
    //            return type.sliceQueryFilterSerializer().deserialize(in, version);
    //        }
    //        else
    //        {
    //            assert b == 1;
    //            return type.namesQueryFilterSerializer().deserialize(in, version);
    //        }
    //    }

    //    public long serializedSize(IDiskAtomFilter filter, int version)
    //    {
    //        int size = 1;
    //        if (filter instanceof SliceQueryFilter)
    //            size += type.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)filter, version);
    //        else
    //            size += type.namesQueryFilterSerializer().serializedSize((NamesQueryFilter)filter, version);
    //        return size;
    //    }
    //}

}
