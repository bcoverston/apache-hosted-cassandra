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
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Interval;

/**
 * A range tombstone is a tombstone that covers a slice/range of rows.
 * <p>
 * Note that in most of the storage engine, a range tombstone is actually represented by its separated
 * opening and closing bound, see {@link RangeTombstoneMarker}. So in practice, this is only used when
 * full partitions are materialized in memory in a {@code Partition} object, and more precisely through
 * the use of a {@code RangeTombstoneList} in a {@code DeletionInfo} object.
 */
public class RangeTombstone
{
    private final Slice slice;
    private final DeletionTime deletion;

    public RangeTombstone(Slice slice, DeletionTime deletion)
    {
        this.slice = slice;
        this.deletion = deletion.takeAlias();
    }

    /**
     * The slice of rows that is deleted by this range tombstone.
     *
     * @return the slice of rows that is deleted by this range tombstone.
     */
    public Slice deletedSlice()
    {
        return slice;
    }

    /**
     * The deletion time for this (range) tombstone.
     *
     * @return the deletion time for this range tombstone.
     */
    public DeletionTime deletionTime()
    {
        return deletion;
    }

    // TODO: do we need ?!
    //public static class Serializer implements ISSTableSerializer<RangeTombstone>
    //{
    //    private final LegacyLayout layout;

    //    public Serializer(LegacyLayout layout)
    //    {
    //        this.layout = layout;
    //    }

    //    public void serializeForSSTable(RangeTombstone t, DataOutputPlus out) throws IOException
    //    {
    //        layout.discardingClusteringSerializer().serialize(t.min, out);
    //        out.writeByte(LegacyLayout.RANGE_TOMBSTONE_MASK);
    //        layout.discardingClusteringSerializer().serialize(t.max, out);
    //        DeletionTime.serializer.serialize(t.data, out);
    //    }

    //    public RangeTombstone deserializeFromSSTable(DataInput in, Version version) throws IOException
    //    {
    //        ClusteringPrefix min = layout.discardingClusteringSerializer().deserialize(in);

    //        int b = in.readUnsignedByte();
    //        assert (b & LegacyLayout.RANGE_TOMBSTONE_MASK) != 0;
    //        return deserializeBody(in, min, version);
    //    }

    //    public RangeTombstone deserializeBody(DataInput in, ClusteringPrefix min, Version version) throws IOException
    //    {
    //        ClusteringPrefix max = layout.discardingClusteringSerializer().deserialize(in);
    //        DeletionTime dt = DeletionTime.serializer.deserialize(in);
    //        return new RangeTombstone(min, max, dt);
    //    }

    //    public void skipBody(DataInput in, Version version) throws IOException
    //    {
    //        layout.discardingClusteringSerializer().deserialize(in);
    //        DeletionTime.serializer.skip(in);
    //    }

    //    public long serializedSizeForSSTable(RangeTombstone t)
    //    {
    //        TypeSizes typeSizes = TypeSizes.NATIVE;
    //        return layout.discardingClusteringSerializer().serializedSize(t.min, typeSizes)
    //             + 1 // serialization flag
    //             + layout.discardingClusteringSerializer().serializedSize(t.max, typeSizes)
    //             + DeletionTime.serializer.serializedSize(t.data, typeSizes);
    //    }
    //}
}
