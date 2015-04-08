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
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * A slice represents the selection of a range of rows.
 * <p>
 * A slice has a start and an end bound that are both (potentially full) clustering prefixes.
 * A slice selects every rows whose clustering is bigger than the slice start prefix but smaller
 * than the end prefix. Both start and end can be either inclusive or exclusive.
 */
public class Slice
{
    public static final Serializer serializer = new Serializer();

    /** The slice selecting all rows (of a given partition) */
    public static final Slice ALL = new Slice(Bound.BOTTOM, Bound.TOP)
    {
        @Override
        public boolean selects(ClusteringComparator comparator, Clustering clustering)
        {
            return true;
        }

        @Override
        public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return true;
        }

        @Override
        public String toString(ClusteringComparator comparator)
        {
            return "ALL";
        }
    };

    private final Bound start;
    private final Bound end;

    private Slice(Bound start, Bound end)
    {
        assert start.isStart() && !end.isStart();
        this.start = start.takeAlias();
        this.end = end.takeAlias();
    }

    public static Slice make(Bound start, Bound end)
    {
        if (start == Bound.BOTTOM && end == Bound.TOP)
            return ALL;

        return new Slice(start, end);
    }

    public static Slice make(ClusteringComparator comparator, Object... values)
    {
        CBuilder builder = CBuilder.create(comparator);
        for (int i = 0; i < values.length; i++)
        {
            Object val = values[i];
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer)val);
            else
                builder.add(val);
        }
        return new Slice(builder.buildBound(true, true), builder.buildBound(false, true));
    }

    public static Slice make(ClusteringComparator comparator, Clustering clustering)
    {
        // This doesn't give us what we want with the clustering prefix
        assert clustering != Clustering.STATIC_CLUSTERING;
        ByteBuffer[] values = extractValues(clustering);
        return new Slice(Bound.inclusiveStartOf(values), Bound.inclusiveEndOf(values));
    }

    private static ByteBuffer[] extractValues(ClusteringPrefix clustering)
    {
        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return values;
    }

    public Bound start()
    {
        return start;
    }

    public Bound end()
    {
        return end;
    }

    /**
     * Return whether the slice is empty.
     *
     * @param comparator the comparator to compare the bounds.
     * @return whether the slice formed is empty or not.
     */
    public boolean isEmpty(ClusteringComparator comparator)
    {
        return isEmpty(comparator, start(), end());
    }

    /**
     * Return whether the slice formed by the two provided bound is empty or not.
     *
     * @param comparator the comparator to compare the bounds.
     * @param start the start for the slice to consider. This must be a start bound.
     * @param end the end for the slice to consider. This must be an end bound.
     * @return whether the slice formed by {@code start} and {@code end} is
     * empty or not.
     */
    public static boolean isEmpty(ClusteringComparator comparator, Slice.Bound start, Slice.Bound end)
    {
        assert start.isStart() && end.isEnd();
        return comparator.compare(end, start) < 0;
    }

    /**
     * Returns whether a given clustering is selected by this slice.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param clustering the clustering to test inclusion of.
     *
     * @return whether {@code clustering} is selected by this slice.
     */
    public boolean selects(ClusteringComparator comparator, Clustering clustering)
    {
        return comparator.compare(start, clustering) <= 0 && comparator.compare(clustering, end) <= 0;
    }

    /**
     * Returns a slice for continuing paging from the last returned clustering prefix.
     *
     * @param comparator the comparator for the table this is a filter for.
     * @param lastReturned the last clustering that was returned for the query we are paging for. The
     * resulting slices will be such that only results coming stricly after {@code lastReturned} are returned
     * (where coming after means "greater than" if {@code !reversed} and "lesser than" otherwise).
     * @param reversed whether the query we're paging for is reversed or not.
     *
     * @return a new slice that selects results coming strictly after {@code lastReturned}, or {@code null} if paging
     * the resulting slice selects nothing (i.e. if it originally selects nothing coming after {@code lastReturned}).
     */
    public Slice forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean reversed)
    {
        if (reversed)
        {
            if (comparator.compare(lastReturned, start) <= 0)
                return null;

            return comparator.compare(end, lastReturned) < 0
                 ? this
                 : new Slice(start, Bound.exclusiveEndOf(extractValues(lastReturned)));
        }
        else
        {
            if (comparator.compare(end, lastReturned) <= 0)
                return null;

            return comparator.compare(lastReturned, start) < 0
                 ? this
                 : new Slice(Bound.exclusiveStartOf(extractValues(lastReturned)), end);
        }
    }

    /**
     * Given the per-clustering column minimum and maximum value a sstable contains, whether or not this slice potentially
     * intersects that sstable or not.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param minClusteringValues the smallest values for each clustering column that a sstable contains.
     * @param maxClusteringValues the biggest values for each clustering column that a sstable contains.
     *
     * @return whether the slice might intersects with the sstable having {@code minClusteringValues} and
     * {@code maxClusteringValues}.
     */
    public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
    {
        // If this slice start after max or end before min, it can't intersect
        if (start.compareTo(comparator, maxClusteringValues) > 0 || end.compareTo(comparator, minClusteringValues) < 0)
            return false;

        // We could safely return true here, but there's a minor optimization: if the first component
        // of the slice is restricted to a single value (typically the slice is [4:5, 4:7]), we can
        // check that the second component falls within the min/max for that component (and repeat for
        // all components).
        for (int j = 0; j < minClusteringValues.size() && j < maxClusteringValues.size(); j++)
        {
            ByteBuffer s = j < start.size() ? start.get(j) : null;
            ByteBuffer f = j < end.size() ? end.get(j) : null;

            // we already know the first component falls within its min/max range (otherwise we wouldn't get here)
            if (j > 0 && (j < end.size() && comparator.compareComponent(j, f, minClusteringValues.get(j)) < 0 ||
                        j < start.size() && comparator.compareComponent(j, s, maxClusteringValues.get(j)) > 0))
                return false;

            // if this component isn't equal in the start and finish, we don't need to check any more
            if (j >= start.size() || j >= end.size() || comparator.compareComponent(j, s, f) != 0)
                break;
        }
        return true;
    }

    public String toString(CFMetaData metadata)
    {
        return toString(metadata.comparator);
    }

    public String toString(ClusteringComparator comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(start.isInclusive() ? "[" : "(");
        for (int i = 0; i < start.size(); i++)
        {
            if (i > 0)
                sb.append(":");
            sb.append(comparator.subtype(i).getString(start.get(i)));
        }
        sb.append(", ");
        for (int i = 0; i < end.size(); i++)
        {
            if (i > 0)
                sb.append(":");
            sb.append(comparator.subtype(i).getString(end.get(i)));
        }
        sb.append(end.isInclusive() ? "]" : ")");
        return sb.toString();
    }

    public static class Serializer
    {
        public void serialize(Slice slice, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            Bound.serializer.serialize(slice.start, out, version, types);
            Bound.serializer.serialize(slice.end, out, version, types);
        }

        public long serializedSize(Slice slice, int version, List<AbstractType<?>> types, TypeSizes sizes)
        {
            return Bound.serializer.serializedSize(slice.start, version, types, sizes)
                 + Bound.serializer.serializedSize(slice.end, version, types, sizes);
        }

        public Slice deserialize(DataInput in, int version, List<AbstractType<?>> types) throws IOException
        {
            Bound start = Bound.serializer.deserialize(in, version, types);
            Bound end = Bound.serializer.deserialize(in, version, types);
            return new Slice(start, end);
        }
    }

    /**
     * The bound of a slice.
     * <p>
     * This can be either a start or an end bound, and this can be either inclusive or exclusive.
     */
    public static abstract class Bound extends AbstractClusteringPrefix
    {
        public static final Serializer serializer = new Serializer();

        /** The smallest start bound, i.e. the one that starts before any row. */
        public static final Bound BOTTOM = inclusiveStartOf();
        /** The biggest end bound, i.e. the one that ends after any row. */
        public static final Bound TOP = inclusiveEndOf();

        public static Bound create(Kind kind, ByteBuffer[] values)
        {
            return new SimpleBound(kind, values);
        }

        public static Kind boundKind(boolean isStart, boolean isInclusive)
        {
            return isStart
                 ? (isInclusive ? Kind.INCL_START_BOUND : Kind.EXCL_START_BOUND)
                 : (isInclusive ? Kind.INCL_END_BOUND : Kind.EXCL_END_BOUND);
        }

        public static Bound inclusiveStartOf(ByteBuffer... values)
        {
            return create(Kind.INCL_START_BOUND, values);
        }

        public static Bound inclusiveEndOf(ByteBuffer... values)
        {
            return create(Kind.INCL_END_BOUND, values);
        }

        public static Bound exclusiveStartOf(ByteBuffer... values)
        {
            return create(Kind.EXCL_START_BOUND, values);
        }

        public static Bound exclusiveEndOf(ByteBuffer... values)
        {
            return create(Kind.EXCL_END_BOUND, values);
        }

        public static Bound exclusiveStartOf(ClusteringPrefix prefix)
        {
            ByteBuffer[] values = new ByteBuffer[prefix.size()];
            for (int i = 0; i < prefix.size(); i++)
                values[i] = prefix.get(i);
            return exclusiveStartOf(values);
        }

        public static Bound inclusiveEndOf(ClusteringPrefix prefix)
        {
            ByteBuffer[] values = new ByteBuffer[prefix.size()];
            for (int i = 0; i < prefix.size(); i++)
                values[i] = prefix.get(i);
            return inclusiveEndOf(values);
        }

        public abstract Bound withNewKind(Kind kind);

        public boolean isStart()
        {
            return kind().isStart();
        }

        public boolean isEnd()
        {
            return !isStart();
        }

        public boolean isInclusive()
        {
            return kind().isInclusive();
        }

        /**
         * Returns the inverse of the current bound.
         * <p>
         * This invert both start into end (and vice-versa) and inclusive into exclusive (and vice-versa).
         *
         * @return the invert of this bound. For instance, if this bound is an exlusive start, this return
         * an inclusive end with the same values.
         */
        public Slice.Bound invert()
        {
            return withNewKind(kind().invert());
        }

        public void digest(MessageDigest digest)
        {
            for (int i = 0; i < size(); i++)
                digest.update(get(i).duplicate());
            FBUtilities.updateWithByte(digest, kind().ordinal());
        }

        public void writeTo(Slice.Bound.Writer writer)
        {
            super.writeTo(writer);
            writer.writeBoundKind(kind());
        }

        // For use by intersects, it's called with the sstable bound opposite to the slice bound
        // (so if the slice bound is a start, it's call with the max sstable bound)
        private int compareTo(ClusteringComparator comparator, List<ByteBuffer> sstableBound)
        {
            for (int i = 0; i < sstableBound.size(); i++)
            {
                // Say the slice bound is a start. It means we're in the case where the max
                // sstable bound is say (1:5) while the slice start is (1). So the start
                // does start before the sstable end bound (and intersect it). It's the exact
                // inverse with a end slice bound.
                if (i >= size())
                    return isStart() ? -1 : 1;

                int cmp = comparator.compareComponent(i, get(i), sstableBound.get(i));
                if (cmp != 0)
                    return cmp;
            }

            // Say the slice bound is a start. I means we're in the case where the max
            // sstable bound is say (1), while the slice start is (1:5). This again means
            // that the slice start before the end bound.
            if (size() > sstableBound.size())
                return isStart() ? -1 : 1;

            // The slice bound is equal to the sstable bound. Results depends on whether the slice is inclusive or not
            return isInclusive() ? 0 : (isStart() ? 1 : -1);
        }

        public String toString(CFMetaData metadata)
        {
            return toString(metadata.comparator);
        }

        public String toString(ClusteringComparator comparator)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(kind()).append("(");
            for (int i = 0; i < size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(comparator.subtype(i).getString(get(i)));
            }
            return sb.append(")").toString();
        }

        // Overriding to get a more precise type
        @Override
        public abstract Bound takeAlias();

        public static Builder builder(int size)
        {
            return new Builder(size);
        }

        public interface Writer extends ClusteringPrefix.Writer
        {
            public void writeBoundKind(Kind kind);
        }

        public static class SimpleBound extends Bound
        {
            private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleBound(Kind.INCL_START_BOUND, new ByteBuffer[0]));

            protected final Kind kind;
            protected final ByteBuffer[] values;

            protected SimpleBound(Kind kind, ByteBuffer[] values)
            {
                this.kind = kind;
                this.values = values;
            }

            public Kind kind()
            {
                return kind;
            }

            public int size()
            {
                return values.length;
            }

            public ByteBuffer get(int i)
            {
                return values[i];
            }

            public Bound withNewKind(Kind kind)
            {
                return new SimpleBound(kind, values);
            }

            @Override
            public long unsharedHeapSize()
            {
                return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
            }

            public Bound takeAlias()
            {
                return this;
            }
        }

        public static class Builder implements Writer
        {
            private final ByteBuffer[] values;
            private Kind kind;
            private int idx;

            private Builder(int size)
            {
                this.values = new ByteBuffer[size];
            }

            public void writeClusteringValue(ByteBuffer value)
            {
                values[idx++] = value;
            }

            public void writeBoundKind(Kind kind)
            {
                this.kind = kind;
            }

            public Slice.Bound build()
            {
                assert idx == values.length;
                return Slice.Bound.create(kind, values);
            }
        }

        /**
         * Serializer for slice bounds.
         * <p>
         * Contrarily to {@code Clustering}, a slice bound can only be a true prefix of the full clustering, so we actually record
         * its size.
         */
        public static class Serializer
        {
            public void serialize(Slice.Bound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
            {
                out.writeByte(bound.kind().ordinal());
                serializeValues(bound, out, version, types);
            }

            public long serializedSize(Slice.Bound bound, int version, List<AbstractType<?>> types, TypeSizes sizes)
            {
                return 1 + serializedSizeValues(bound, version, types, sizes);
            }

            public Slice.Bound deserialize(DataInput in, int version, List<AbstractType<?>> types) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];
                return deserializeValues(in, kind, version, types);
            }

            public void serializeValues(Slice.Bound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
            {
                out.writeShort(bound.size());
                ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
            }

            public long serializedSizeValues(Slice.Bound bound, int version, List<AbstractType<?>> types, TypeSizes sizes)
            {
                return sizes.sizeof((short)bound.size()) + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types, sizes);
            }

            public void deserializeValues(DataInput in, Bound.Kind kind, int version, List<AbstractType<?>> types, Writer writer) throws IOException
            {
                int size = in.readUnsignedShort();
                ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types, writer);
                writer.writeBoundKind(kind);
            }

            public Slice.Bound deserializeValues(DataInput in, Kind kind, int version, List<AbstractType<?>> types) throws IOException
            {
                int size = in.readUnsignedShort();
                if (size == 0)
                    return kind.isStart() ? BOTTOM : TOP;

                Builder builder = builder(size);
                ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types, builder);
                builder.writeBoundKind(kind);
                return builder.build();
            }

        }
    }
}
