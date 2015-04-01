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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * Represents the legacy layouts: dense/sparse and simple/compound.
 *
 * This is only use to serialize/deserialize the old format.
 */
public class LegacyLayout
{
    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    public final static int RANGE_TOMBSTONE_MASK = 0x10;

    private final CFMetaData metadata;

    //private final RangeTombstone.Serializer rangeTombstoneSerializer;
    //private final DeletionInfo.Serializer deletionInfoSerializer;

    public LegacyLayout(CFMetaData metadata)
    {
        this.metadata = metadata;

        //this.rangeTombstoneSerializer = new RangeTombstone.Serializer(this);
        //this.deletionInfoSerializer = new DeletionInfo.Serializer(this);
    }

    public AbstractType<?> makeLegacyComparator()
    {
        ClusteringComparator comparator = metadata.comparator;
        if (!metadata.isCompound())
        {
            if (metadata.isDense())
            {
                assert comparator.size() == 1;
                return comparator.subtype(0);
            }
            else
            {
                assert comparator.size() == 0;
                return metadata.columnNameComparator;
            }
        }

        boolean hasCollections = metadata.hasCollectionColumns();
        List<AbstractType<?>> types = new ArrayList<>(comparator.size() + (metadata.isDense() ? 0 : 1) + (hasCollections ? 1 : 0));

        types.addAll(comparator.subtypes());

        if (!metadata.isDense())
        {
            types.add(metadata.columnNameComparator);
            if (hasCollections)
            {
                Map<ByteBuffer, CollectionType> defined = new HashMap<>();
                for (ColumnDefinition def : metadata.partitionColumns())
                {
                    if (def.type instanceof CollectionType && def.type.isMultiCell())
                        defined.put(def.name.bytes, (CollectionType)def.type);
                }
                types.add(ColumnToCollectionType.getInstance(defined));
            }
        }
        return CompositeType.getInstance(types);
    }

    public Deserializer newDeserializer(DataInput in, Version version)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // Note that for the old layout, this will actually discard the cellname parts that are not strictly 
    // part of the clustering prefix. Don't use this if that's not what you want.
    public ISerializer<ClusteringPrefix> clusteringPrefixSerializer(final Version version, final SerializationHeader header)
    {
        if (!version.storeRows())
            throw new UnsupportedOperationException();

        return new ISerializer<ClusteringPrefix>()
        {
            public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
            {
                ClusteringPrefix.serializer.serialize(clustering, out, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public ClusteringPrefix deserialize(DataInput in) throws IOException
            {
                return ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public long serializedSize(ClusteringPrefix clustering, TypeSizes sizes)
            {
                return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(), header.clusteringTypes(), sizes);
            }
        };
    }

    public IndexInfo.Serializer indexSerializer(Version version)
    {
        return new IndexInfo.Serializer(this, version);
    }

    // This may return a null column definition if the cellname doesn't contain that part.
    public Pair<Clustering, ColumnDefinition> decodeCellName(ByteBuffer cellname)
    {
        if (!cellname.hasRemaining())
            return Pair.create(Clustering.EMPTY, null);

        Clustering clustering = decodeClustering(cellname);

        if (metadata.isDense())
            return Pair.create(clustering, metadata.compactValueColumn());

        ByteBuffer column = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size()) : cellname;
        if (column == null)
                return Pair.create(clustering, null);

        ColumnDefinition def = metadata.getColumnDefinition(column);
        if (def == null)
            throw new IllegalArgumentException("Unknown column " + UTF8Type.instance.getString(column));

        return Pair.create(clustering, def);
    }

    public Clustering decodeClustering(ByteBuffer value)
    {
        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(value)
                                    : Collections.singletonList(value);

        int csize = metadata.comparator.size();
        return csize == 0
             ? Clustering.EMPTY
             : new SimpleClustering(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
    }

    public ByteBuffer encodeClustering(Clustering clustering)
    {
        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return CompositeType.build(values);
    }

    //public void deserializeCellBody(DataInput in, DeserializedCell cell, ByteBuffer collectionElement, int mask, Flag flag, int expireBefore)
    //throws IOException
    //{
    //    if ((mask & COUNTER_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //        ///long timestampOfLastDelete = in.readLong();
    //        ///long ts = in.readLong();
    //        ///ByteBuffer value = ByteBufferUtil.readWithLength(in);
    //        ///return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
    //    }
    //    else if ((mask & COUNTER_UPDATE_MASK) != 0)
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //    else if ((mask & EXPIRATION_MASK) != 0)
    //    {
    //        assert collectionElement == null;
    //        cell.isCounter = false;
    //        cell.ttl = in.readInt();
    //        cell.localDeletionTime = in.readInt();
    //        cell.timestamp = in.readLong();
    //        cell.value = ByteBufferUtil.readWithLength(in);
    //        cell.path = null;

    //        // Transform expired cell to tombstone (as it basically saves the value)
    //        if (cell.localDeletionTime < expireBefore && flag != Flag.PRESERVE_SIZE)
    //        {
    //            // The column is now expired, we can safely return a simple tombstone. Note that
    //            // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
    //            // we'll fulfil our responsibility to repair.  See discussion at
    //            // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
    //            cell.localDeletionTime = cell.localDeletionTime - cell.ttl;
    //            cell.ttl = 0;
    //            cell.value = null;
    //        }

    //    }
    //    else
    //    {
    //        cell.isCounter = false;
    //        cell.ttl = 0;
    //        cell.path = null;

    //        cell.timestamp = in.readLong();
    //        ByteBuffer value = ByteBufferUtil.readWithLength(in);

    //        boolean isDeleted = (mask & DELETION_MASK) != 0;
    //        cell.value = isDeleted ? null : value;
    //        cell.localDeletionTime = isDeleted ? ByteBufferUtil.toInt(value) : Integer.MAX_VALUE;
    //    }
    //}

    public void skipCellBody(DataInput in, int mask)
    throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else if ((mask & EXPIRATION_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else
            FileUtils.skipBytesFully(in, 8);

        int length = in.readInt();
        FileUtils.skipBytesFully(in, length);
    }

    //public abstract IVersionedSerializer<ColumnSlice> sliceSerializer();
    //public abstract IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();

    //public DeletionInfo.Serializer deletionInfoSerializer()
    //{
    //    return deletionInfoSerializer;
    //}

    //public RangeTombstone.Serializer rangeTombstoneSerializer()
    //{
    //    return rangeTombstoneSerializer;
    //}

    //public static class LegacyAtomDeserializer extends AtomDeserializer
    //{
    //    private final Deserializer nameDeserializer;

    //    private RangeTombstone openTombstone;

    //    private final ReusableRangeTombstoneMarker marker;
    //    private final ReusableRow row;

    //    private LegacyLayout.DeserializedCell cell;

    //    public LegacyAtomDeserializer(CFMetaData metadata,
    //                                  DataInput in,
    //                                  LegacyLayout.Flag flag,
    //                                  int expireBefore,
    //                                  Version version,
    //                                  Columns columns)
    //    {
    //        super(metadata, in, flag, expireBefore, version, columns);
    //        this.nameDeserializer = metadata.layout().newDeserializer(in, version);
    //        this.marker = new ReusableRangeTombstoneMarker();
    //        this.row = new ReusableRow(columns);
    //    }

    //    public boolean hasNext() throws IOException
    //    {
    //        return hasUnprocessed() || nameDeserializer.hasNext();
    //    }

    //    public boolean hasUnprocessed() throws IOException
    //    {
    //        return openTombstone != null || nameDeserializer.hasUnprocessed();
    //    }

    //    public int compareNextTo(Clusterable prefix) throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return metadata.comparator.compare(openTombstone.max, prefix);

    //        return nameDeserializer.compareNextTo(prefix);
    //    }

    //    public Atom readNext() throws IOException
    //    {
    //        if (openTombstone != null && nameDeserializer.compareNextTo(openTombstone.max) > 0)
    //            return marker.setTo(openTombstone.max, false, openTombstone.data);

    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            // TODO: deal with new style RT
    //            openTombstone = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //            return marker.setTo(openTombstone.min, true, openTombstone.data);
    //        }

    //        Row.Writer writer = row.writer();
    //        writer.writeClustering(prefix);

    //        // If there is a row marker, it's the first cell
    //        ByteBuffer columnName = nameDeserializer.getNextColumnName();
    //        if (columnName != null && !columnName.hasRemaining())
    //        {
    //            metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //            writer.writeTimestamp(cell.timestamp());
    //        }
    //        else
    //        {
    //            writer.writeTimestamp(Long.MIN_VALUE);
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if (columns.contains(column))
    //            {
    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //            else
    //            {
    //                metadata.layout().skipCellBody(in, b);
    //            }
    //        }

    //        // Read the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.readNextClustering();
    //            b = in.readUnsignedByte();
    //            ColumnDefinition column = getDefinition(nameDeserializer.getNextColumnName());
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //                    continue;
    //                }

    //                // This is a collection tombstone
    //                RangeTombstone rt = metadata.layout().rangeTombstoneSerializer().deserializeBody(in, prefix, version);
    //                // TODO: we could assert that the min and max are what we think they are. Just in case
    //                // someone thrift side has done something *really* nasty.
    //                writer.writeComplexDeletion(column, rt.data);
    //            }
    //            else
    //            {
    //                if (!columns.contains(column))
    //                {
    //                    metadata.layout().skipCellBody(in, b);
    //                    continue;
    //                }

    //                cell.column = column;
    //                metadata.layout().deserializeCellBody(in, cell, nameDeserializer.getNextCollectionElement(), b, flag, expireBefore);
    //                Cells.write(cell, writer);
    //            }
    //        }
    //        return row;
    //    }

    //    private ColumnDefinition getDefinition(ByteBuffer columnName)
    //    {
    //        // For non-CQL3 layouts, every defined column metadata is handled by the static row
    //        if (!metadata.layout().isCQL3Layout())
    //            return metadata.compactValueColumn();

    //        return metadata.getColumnDefinition(columnName);
    //    }

    //    public void skipNext() throws IOException
    //    {
    //        Clustering prefix = nameDeserializer.readNextClustering();
    //        int b = in.readUnsignedByte();
    //        if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //        {
    //            metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            return;
    //        }

    //        metadata.layout().skipCellBody(in, b);

    //        // Skip the rest of the cells belonging to this CQL row
    //        while (nameDeserializer.hasNext() && nameDeserializer.compareNextPrefixTo(prefix) == 0)
    //        {
    //            nameDeserializer.skipNext();
    //            b = in.readUnsignedByte();
    //            if ((b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0)
    //                metadata.layout().rangeTombstoneSerializer().skipBody(in, version);
    //            else
    //                metadata.layout().skipCellBody(in, b);
    //        }
    //    }
    //}

    public interface Deserializer
    {
        /**
         * Whether this deserializer is done or not, i.e. whether we're reached the end of row marker.
         */
        public boolean hasNext() throws IOException;
        /**
         * Whether or not some name has been read but not consumed by readNext.
         */
        public boolean hasUnprocessed() throws IOException;
        /**
         * Comparare the next name to read to the provided Composite.
         * This does not consume the next name.
         */
        public int compareNextTo(Clusterable composite) throws IOException;
        public int compareNextPrefixTo(Clustering prefix) throws IOException;
        /**
         * Actually consume the next name and return it.
         */
        public Clustering readNextClustering() throws IOException;
        public ByteBuffer getNextColumnName();
        public ByteBuffer getNextCollectionElement();

        /**
         * Skip the next name (consuming it). This skip all the name (clustering, column name and collection element).
         */
        public void skipNext() throws IOException;
    }

    //public static class DeserializedCell extends AbstractCell
    //{
    //    public ColumnDefinition column;
    //    private boolean isCounter;
    //    private ByteBuffer value;
    //    private long timestamp;
    //    private int localDeletionTime;
    //    private int ttl;
    //    private CellPath path;

    //    public ColumnDefinition column()
    //    {
    //        return column;
    //    }

    //    public boolean isCounterCell()
    //    {
    //        return isCounter;
    //    }

    //    public ByteBuffer value()
    //    {
    //        return value;
    //    }

    //    public long timestamp()
    //    {
    //        return timestamp;
    //    }

    //    public int localDeletionTime()
    //    {
    //        return localDeletionTime;
    //    }

    //    public int ttl()
    //    {
    //        return ttl;
    //    }

    //    public CellPath path()
    //    {
    //        return path;
    //    }

    //    public Cell takeAlias()
    //    {
    //        // TODO
    //        throw new UnsupportedOperationException();
    //    }
    //}

    // From OnDiskAtom
    //public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    //{
    //    private final CellNameType type;

    //    public Serializer(CellNameType type)
    //    {
    //        this.type = type;
    //    }

    //    public void serializeForSSTable(OnDiskAtom atom, DataOutputPlus out) throws IOException
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            type.columnSerializer().serialize((Cell)atom, out);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            type.rangeTombstoneSerializer().serializeForSSTable((RangeTombstone)atom, out);
    //        }
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
    //    {
    //        return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    //    }

    //    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
    //    {
    //        Composite name = type.serializer().deserialize(in);
    //        if (name.isEmpty())
    //        {
    //            // SSTableWriter.END_OF_ROW
    //            return null;
    //        }

    //        int b = in.readUnsignedByte();
    //        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
    //            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
    //        else
    //            return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    //    }

    //    public long serializedSizeForSSTable(OnDiskAtom atom)
    //    {
    //        if (atom instanceof Cell)
    //        {
    //            return type.columnSerializer().serializedSize((Cell)atom, TypeSizes.NATIVE);
    //        }
    //        else
    //        {
    //            assert atom instanceof RangeTombstone;
    //            return type.rangeTombstoneSerializer().serializedSizeForSSTable((RangeTombstone)atom);
    //        }
    //    }
    //}
}
