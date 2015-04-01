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

import java.io.DataInput;
import java.io.IOException;
import java.io.IOError;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Serialize/Deserialize an atom iterator.
 *
 * The serialization is composed of a header, follows by the atom of the iterator serialized
 * until we read the end of the partition (see AtomSerializer for details). The header itself
 * is:
 *     <cfid><key><flags><s_header>[<partition_deletion>][<static_row>]
 * where:
 *     <cfid> is the table cfid.
 *     <key> is the partition key.
 *     <flags> contains bit flags. Each flag is set if it's corresponding bit is set. From rightmost
 *         bit to leftmost one, the flags are:
 *         - is empty: whether the iterator is empty. If so, nothing follows the <flags>
 *         - is reversed: whether the atoms a serialized in reversed clustering order
 *         - has partition deletion: whether or not there is a <partition_deletion> following
 *         - has static row: whether or not there is a <static_row> following
 *         - has row estimate: whether or not there is a <row_estimate> following
 *     <s_header> is the SerializationHeader. More precisely it's
 *           <min_timetamp><min_localDelTime><min_ttl>[<static_columns>]<columns>
 *         where:
 *           - <min_timestamp> is the base timestamp used for delta-encoding timestamps
 *           - <min_localDelTime> is the base localDeletionTime used for delta-encoding local deletion times
 *           - <min_ttl> is the base localDeletionTime used for delta-encoding ttls
 *           - <static_columns> is the static columns if a static row is present. It's
 *             the number of columns as an unsigned short, followed by the column names.
 *           - <columns> is the columns of the rows of the iterator. It's serialized as <static_columns>.
 *     <nowInSec> is the value for nowInSec() for this iterator.
 *     <partition_deletion> is the deletion time for the partition (delta-encoded)
 *     <static_row> is the static row for this partition as serialized by AtomSerializer.
 *     <row_estimate> is the (potentially estimated) number of rows serialized. This is only use for
 *         the purpose of some sizing on the receiving end and should not be relied upon too strongly.
 *
 * !!! Please note that the serialized value depends on the schema and as such should not be used as is if
 *     it might be deserialized after the schema as changed !!!
 * TODO: we should add a flag to include the relevant metadata in the header for commit log etc.....
 */
public class AtomIteratorSerializer
{
    protected static final Logger logger = LoggerFactory.getLogger(AtomIteratorSerializer.class);

    private static final int IS_EMPTY               = 0x01;
    private static final int IS_REVERSED            = 0x02;
    private static final int HAS_PARTITION_DELETION = 0x04;
    private static final int HAS_STATIC_ROW         = 0x08;
    private static final int HAS_ROW_ESTIMATE       = 0x10;

    public static final AtomIteratorSerializer serializer = new AtomIteratorSerializer();

    public void serialize(AtomIterator iterator, DataOutputPlus out, int version) throws IOException
    {
        serialize(iterator, out, version, -1);
    }

    public void serialize(AtomIterator iterator, DataOutputPlus out, int version, int rowEstimate) throws IOException
    {
        SerializationHeader header = new SerializationHeader(iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats(),
                                                             false);
        serialize(iterator, out, header, version, rowEstimate);
    }

    public void serialize(AtomIterator iterator, DataOutputPlus out, SerializationHeader header, int version, int rowEstimate) throws IOException
    {
        CFMetaData.serializer.serialize(iterator.metadata(), out, version);
        header.keyType().writeValue(iterator.partitionKey().getKey(), out);

        int flags = 0;
        if (iterator.isReverseOrder())
            flags |= IS_REVERSED;

        if (AtomIterators.isEmpty(iterator))
        {
            out.writeByte((byte)(flags | IS_EMPTY));
            return;
        }

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        if (!partitionDeletion.isLive())
            flags |= HAS_PARTITION_DELETION;
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            flags |= HAS_STATIC_ROW;

        if (rowEstimate >= 0)
            flags |= HAS_ROW_ESTIMATE;

        out.writeByte((byte)flags);

        SerializationHeader.serializer.serializeForMessaging(header, out, hasStatic);

        out.writeInt(iterator.nowInSec());

        if (!partitionDeletion.isLive())
            writeDelTime(partitionDeletion, header, out);

        if (hasStatic)
            AtomSerializer.serializer.serialize(staticRow, header, out, version);

        if (rowEstimate >= 0)
            out.writeInt(rowEstimate);

        while (iterator.hasNext())
            AtomSerializer.serializer.serialize(iterator.next(), header, out, version);
        AtomSerializer.serializer.writeEndOfPartition(out);
    }

    // Please note that this consume the iterator, and as such should not be called unless we have a simple way to
    // recreate an iterator for both serialize and serializedSize, which is mostly only PartitionUpdate
    public long serializedSize(AtomIterator iterator, int version, int rowEstimate, TypeSizes sizes)
    {
        SerializationHeader header = new SerializationHeader(iterator.metadata(),
                                                             iterator.columns(),
                                                             iterator.stats(),
                                                             false);

        assert rowEstimate >= 0;

        long size = CFMetaData.serializer.serializedSize(iterator.metadata(), version, sizes)
                  + header.keyType().writtenLength(iterator.partitionKey().getKey(), sizes)
                  + 1; // flags

        if (AtomIterators.isEmpty(iterator))
            return size;

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        size += SerializationHeader.serializer.serializedSizeForMessaging(header, sizes, hasStatic);

        size += sizes.sizeof(iterator.nowInSec());

        if (!partitionDeletion.isLive())
            size += delTimeSerializedSize(partitionDeletion, header, sizes);

        if (hasStatic)
            size += AtomSerializer.serializer.serializedSize(staticRow, header, version, sizes);

        if (rowEstimate >= 0)
            size += sizes.sizeof(rowEstimate);

        while (iterator.hasNext())
            size += AtomSerializer.serializer.serializedSize(iterator.next(), header, version, sizes);
        size += AtomSerializer.serializer.serializedSizeEndOfPartition(sizes);

        return size;
    }

    public Header deserializeHeader(DataInput in, int version, SerializationHelper.Flag flag) throws IOException
    {
        CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
        DecoratedKey key = StorageService.getPartitioner().decorateKey(metadata.getKeyValidator().readValue(in));
        int flags = in.readUnsignedByte();
        boolean isReversed = (flags & IS_REVERSED) != 0;
        if ((flags & IS_EMPTY) != 0)
        {
            SerializationHeader sh = new SerializationHeader(metadata, PartitionColumns.NONE, AtomStats.NO_STATS, false);
            return new Header(sh, metadata, key, isReversed, true, null, null, 0, FBUtilities.nowInSeconds());
        }

        boolean hasPartitionDeletion = (flags & HAS_PARTITION_DELETION) != 0;
        boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;
        boolean hasRowEstimate = (flags & HAS_ROW_ESTIMATE) != 0;

        SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, hasStatic);

        int nowInSec = in.readInt();
        DeletionTime partitionDeletion = hasPartitionDeletion ? readDelTime(in, header) : DeletionTime.LIVE;

        Row staticRow = Rows.EMPTY_STATIC_ROW;
        if (hasStatic)
            staticRow = AtomSerializer.serializer.deserializeStaticRow(in, header, new SerializationHelper(version, flag, nowInSec));

        int rowEstimate = hasRowEstimate ? in.readInt() : -1;
        return new Header(header, metadata, key, isReversed, false, partitionDeletion, staticRow, rowEstimate, nowInSec);
    }

    public void deserializeAtoms(DataInput in, SerializationHelper helper, SerializationHeader header, Row.Writer rowWriter, RangeTombstoneMarker.Writer markerWriter) throws IOException
    {
        while (AtomSerializer.serializer.deserialize(in, header, helper, rowWriter, markerWriter) != null);
    }

    public AtomIterator deserialize(final DataInput in, int version, SerializationHelper.Flag flag) throws IOException
    {
        final Header h = deserializeHeader(in, version, flag);

        if (h.isEmpty)
            return AtomIterators.emptyIterator(h.metadata, h.key, h.isReversed, h.nowInSec);

        final int clusteringSize = h.metadata.clusteringColumns().size();
        final SerializationHelper helper = new SerializationHelper(version, flag, h.nowInSec);

        return new AbstractAtomIterator(h.metadata, h.key, h.partitionDeletion, h.sHeader.columns(), h.staticRow, h.isReversed, h.sHeader.stats(), h.nowInSec)
        {
            private final ReusableRow row = new ReusableRow(clusteringSize, h.sHeader.columns().regulars, helper.nowInSec);
            private final ReusableRangeTombstoneMarker marker = new ReusableRangeTombstoneMarker(clusteringSize);

            protected Atom computeNext()
            {
                try
                {
                    Atom.Kind kind = AtomSerializer.serializer.deserialize(in, h.sHeader, helper, row.writer(), marker.writer());
                    if (kind == null)
                        return endOfData();

                    return kind == Atom.Kind.ROW ? row : marker;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public static void writeDelTime(DeletionTime dt, SerializationHeader header, DataOutputPlus out) throws IOException
    {
        out.writeLong(header.encodeTimestamp(dt.markedForDeleteAt()));
        out.writeInt(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static long delTimeSerializedSize(DeletionTime dt, SerializationHeader header, TypeSizes sizes)
    {
        return sizes.sizeof(header.encodeTimestamp(dt.markedForDeleteAt()))
             + sizes.sizeof(header.encodeDeletionTime(dt.localDeletionTime()));
    }

    public static DeletionTime readDelTime(DataInput in, SerializationHeader header) throws IOException
    {
        long markedAt = header.decodeTimestamp(in.readLong());
        int localDelTime = header.decodeDeletionTime(in.readInt());
        return new SimpleDeletionTime(markedAt, localDelTime);
    }

    public static void skipDelTime(DataInput in, SerializationHeader header) throws IOException
    {
        // Note that since we might use VINT, we shouldn't assume the size of a long or an int
        in.readLong();
        in.readInt();
    }

    public static class Header
    {
        public final SerializationHeader sHeader;
        public final CFMetaData metadata;
        public final DecoratedKey key;
        public final boolean isReversed;
        public final boolean isEmpty;
        public final DeletionTime partitionDeletion;
        public final Row staticRow;
        public final int rowEstimate; // -1 if no estimate
        public final int nowInSec;

        private Header(SerializationHeader sHeader,
                       CFMetaData metadata,
                       DecoratedKey key,
                       boolean isReversed,
                       boolean isEmpty,
                       DeletionTime partitionDeletion,
                       Row staticRow,
                       int rowEstimate,
                       int nowInSec)
        {
            this.sHeader = sHeader;
            this.metadata = metadata;
            this.key = key;
            this.isReversed = isReversed;
            this.isEmpty = isEmpty;
            this.partitionDeletion = partitionDeletion;
            this.staticRow = staticRow;
            this.rowEstimate = rowEstimate;
            this.nowInSec = nowInSec;
        }
    }
}
