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

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Represents the selection of multiple range of rows within a partition.
 * <p>
 * A {@code Slices} is basically a list of {@code Slice}, though those are guaranteed to be non-overlapping
 * and always in clustering order.
 */
public abstract class Slices implements Iterable<Slice>
{
    public static final Serializer serializer = new Serializer();

    /** Slices selecting all the rows of a partition. */
    public static final Slices ALL = new SelectAllSlices();
    /** Slices selecting no rows in a partition. */
    public static final Slices NONE = new SelectNoSlices();

    protected Slices()
    {
    }

    /**
     * Creates a {@code Slices} object that contains a single slice.
     *
     * @param comparator the comparator for the table {@code slice} is a slice of.
     * @param slice the single slice that the return object should contains.
     *
     * @return the newly created {@code Slices} object.
     */
    public static Slices with(ClusteringComparator comparator, Slice slice)
    {
        if (slice.start() == Slice.Bound.BOTTOM && slice.end() == Slice.Bound.TOP)
            return Slices.ALL;

        assert comparator.compare(slice.start(), slice.end()) <= 0;
        return new ArrayBackedSlices(comparator, new Slice[]{ slice });
    }

    /**
     * Whether the slices has a lower bound, that is whether it's first slice start is {@code Slice.BOTTOM}.
     *
     * @return whether the slices has a lower bound.
     */
    public abstract boolean hasLowerBound();

    /**
     * Whether the slices has an upper bound, that is whether it's last slice end is {@code Slice.TOP}.
     *
     * @return whether the slices has an upper bound.
     */
    public abstract boolean hasUpperBound();

    /**
     * The number of slice this object contains.
     *
     * @return the number of slice this object contains.
     */
    public abstract int size();

    /**
     * Returns the ith slice of this {@code Slices} object.
     *
     * @return the ith slice of this object.
     */
    public abstract Slice get(int i);

    /**
     * Returns slices for continuing the paging of those slices given the last returned clustering prefix.
     *
     * @param comparator the comparator for the table this is a filter for.
     * @param lastReturned the last clustering that was returned for the query we are paging for. The
     * resulting slices will be such that only results coming stricly after {@code lastReturned} are returned
     * (where coming after means "greater than" if {@code !reversed} and "lesser than" otherwise).
     * @param reversed whether the query we're paging for is reversed or not.
     *
     * @return new slices that select results coming after {@code lastReturned}.
     */
    public abstract Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean reversed);

    /**
     * An object that allows to test whether rows are selected by this {@code Slices} objects assuming those rows
     * are tested in clustering order.
     *
     * @param reversed if true, the rows passed to the returned object will be assumed to be in reversed clustering
     * order, otherwise they should be in clustering order.
     *
     * @return an object that tests for selection of rows by this {@code Slices} object.
     */
    public abstract InOrderTester inOrderTester(boolean reversed);

    /**
     * Whether a given clustering (row) is selected by this {@code Slices} object.
     *
     * @param clustering the clustering to test for selection.
     *
     * @return whether a given clustering (row) is selected by this {@code Slices} object.
     */
    public abstract boolean selects(Clustering clustering);


    /**
     * Given the per-clustering column minimum and maximum value a sstable contains, whether or not this slices potentially
     * intersects that sstable or not.
     *
     * @param minClusteringValues the smallest values for each clustering column that a sstable contains.
     * @param maxClusteringValues the biggest values for each clustering column that a sstable contains.
     *
     * @return whether the slices might intersects with the sstable having {@code minClusteringValues} and
     * {@code maxClusteringValues}.
     */
    public abstract boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues);

    /**
     * Given a sliceable atom iterator, returns an atom iterator that only return atoms selected by the slice of
     * this {@code Slices} object.
     *
     * @param iter the sliceable iterator to filter.
     *
     * @return an iterator that only returns the atom of {@code iter} that are selected by those slices.
     */
    public abstract AtomIterator makeSliceIterator(SliceableAtomIterator iter);

    public abstract String toCQLString(CFMetaData metadata);

    /**
     * In simple object that allows to test the inclusion of rows in those slices assuming those rows
     * are passed (to {@link #includes}) in clustering order (or reverse clustering ordered, depending
     * of the argument passed to {@link #inOrderTester}).
     */
    public interface InOrderTester
    {
        public boolean includes(Clustering value);
        public boolean isDone();
    }

    /**
     * Builder to create {@code Slices} objects.
     */
    public static class Builder
    {
        private final ClusteringComparator comparator;

        private final List<Slice> slices;

        public Builder(ClusteringComparator comparator)
        {
            this.comparator = comparator;
            this.slices = new ArrayList<>();
        }

        public Builder(ClusteringComparator comparator, int initialSize)
        {
            this.comparator = comparator;
            this.slices = new ArrayList<>(initialSize);
        }

        public Builder add(Slice.Bound start, Slice.Bound end)
        {
            return add(Slice.make(start, end));
        }

        public Builder add(Slice slice)
        {
            assert slices.size() == 0 || comparator.compare(slices.get(slices.size()-1).end(), slice.start()) <= 0;
            assert comparator.compare(slice.start(), slice.end()) <= 0;
            slices.add(slice);
            return this;
        }

        public int size()
        {
            return slices.size();
        }

        public Slices build()
        {
            return slices.isEmpty() ? NONE : new ArrayBackedSlices(comparator, slices.toArray(new Slice[slices.size()]));
        }
    }

    public static class Serializer
    {
        public void serialize(Slices slices, DataOutputPlus out, int version) throws IOException
        {
            int size = slices.size();
            out.writeInt(size);

            if (size == 0)
                return;

            List<AbstractType<?>> types = slices == ALL
                                        ? Collections.<AbstractType<?>>emptyList()
                                        : ((ArrayBackedSlices)slices).comparator.subtypes();

            for (Slice slice : slices)
                Slice.serializer.serialize(slice, out, version, types);
        }

        public long serializedSize(Slices slices, int version, TypeSizes sizes)
        {
            long size = sizes.sizeof(slices.size());

            if (slices.size() == 0)
                return size;

            List<AbstractType<?>> types = slices instanceof SelectAllSlices
                                        ? Collections.<AbstractType<?>>emptyList()
                                        : ((ArrayBackedSlices)slices).comparator.subtypes();

            for (Slice slice : slices)
                size += Slice.serializer.serializedSize(slice, version, types, sizes);

            return size;
        }

        public Slices deserialize(DataInput in, int version, CFMetaData metadata) throws IOException
        {
            int size = in.readInt();

            if (size == 0)
                return NONE;

            Slice[] slices = new Slice[size];
            for (int i = 0; i < size; i++)
                slices[i] = Slice.serializer.deserialize(in, version, metadata.comparator.subtypes());

            if (size == 1 && slices[0].start() == Slice.Bound.BOTTOM && slices[0].end() == Slice.Bound.TOP)
                return ALL;

            return new ArrayBackedSlices(metadata.comparator, slices);
        }
    }

    /**
     * Simple {@code Slices} implementation that stores its slices in an array.
     */
    private static class ArrayBackedSlices extends Slices
    {
        private final ClusteringComparator comparator;

        private final Slice[] slices;

        private ArrayBackedSlices(ClusteringComparator comparator, Slice[] slices)
        {
            this.comparator = comparator;
            this.slices = slices;
        }

        public int size()
        {
            return slices.length;
        }

        public boolean hasLowerBound()
        {
            return slices[0].start().size() != 0;
        }

        public boolean hasUpperBound()
        {
            return slices[slices.length - 1].end().size() != 0;
        }

        public Slice get(int i)
        {
            return slices[i];
        }

        public boolean selects(Clustering clustering)
        {
            for (int i = 0; i < slices.length; i++)
            {
                Slice slice = slices[i];
                if (comparator.compare(clustering, slice.start()) < 0)
                    return false;

                if (comparator.compare(clustering, slice.end()) <= 0)
                    return true;
            }
            return false;
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return reversed ? new InReverseOrderTester() : new InForwardOrderTester();
        }

        public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean reversed)
        {
            return reversed ? forReversePaging(comparator, lastReturned) : forForwardPaging(comparator, lastReturned);
        }

        private Slices forForwardPaging(ClusteringComparator comparator, Clustering lastReturned)
        {
            for (int i = 0; i < slices.length; i++)
            {
                Slice slice = slices[i];
                Slice newSlice = slice.forPaging(comparator, lastReturned, false);
                if (newSlice == null)
                    continue;

                if (slice == newSlice && i == 0)
                    return this;

                ArrayBackedSlices newSlices = new ArrayBackedSlices(comparator, Arrays.copyOfRange(slices, i, slices.length));
                newSlices.slices[0] = newSlice;
                return newSlices;
            }
            return Slices.NONE;
        }

        private Slices forReversePaging(ClusteringComparator comparator, Clustering lastReturned)
        {
            for (int i = slices.length - 1; i >= 0; i--)
            {
                Slice slice = slices[i];
                Slice newSlice = slice.forPaging(comparator, lastReturned, true);
                if (newSlice == null)
                    continue;

                if (slice == newSlice && i == slices.length - 1)
                    return this;

                ArrayBackedSlices newSlices = new ArrayBackedSlices(comparator, Arrays.copyOfRange(slices, 0, i + 1));
                newSlices.slices[i] = newSlice;
                return newSlices;
            }
            return Slices.NONE;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            for (Slice slice : this)
            {
                if (slice.intersects(comparator, minClusteringValues, maxClusteringValues))
                    return true;
            }
            return false;
        }

        public AtomIterator makeSliceIterator(final SliceableAtomIterator iter)
        {
            return new WrappingAtomIterator(iter)
            {
                private int nextSlice = iter.isReverseOrder() ? slices.length - 1 : 0;
                private Iterator<Atom> currentSliceIterator = Collections.emptyIterator();

                private Atom next;

                @Override
                public boolean hasNext()
                {
                    prepareNext();
                    return next != null;
                }

                @Override
                public Atom next()
                {
                    prepareNext();
                    Atom toReturn = next;
                    next = null;
                    return toReturn;
                }

                private boolean hasMoreSlice()
                {
                    return isReverseOrder()
                         ? nextSlice >= 0
                         : nextSlice < slices.length;
                }

                private Slice popNextSlice()
                {
                    return slices[isReverseOrder() ? nextSlice-- : nextSlice++];
                }

                private void prepareNext()
                {
                    if (next != null)
                        return;

                    while (true)
                    {
                        if (currentSliceIterator.hasNext())
                        {
                            next = currentSliceIterator.next();
                            return;
                        }

                        if (!hasMoreSlice())
                            return;

                        currentSliceIterator = iter.slice(popNextSlice());
                    }
                }
            };
        }

        public Iterator<Slice> iterator()
        {
            return Iterators.forArray(slices);
        }

        private class InForwardOrderTester implements InOrderTester
        {
            private int idx;
            private boolean inSlice;

            public boolean includes(Clustering value)
            {
                while (idx < slices.length)
                {
                    if (!inSlice)
                    {
                        int cmp = comparator.compare(value, slices[idx].start());
                        // value < start
                        if (cmp < 0)
                            return false;

                        inSlice = true;

                        if (cmp == 0)
                            return true;
                    }

                    // Here, start < value and inSlice
                    if (comparator.compare(value, slices[idx].end()) <= 0)
                        return true;

                    ++idx;
                    inSlice = false;
                }
                return false;
            }

            public boolean isDone()
            {
                return idx >= slices.length;
            }
        }

        private class InReverseOrderTester implements InOrderTester
        {
            private int idx;
            private boolean inSlice;

            public InReverseOrderTester()
            {
                this.idx = slices.length - 1;
            }

            public boolean includes(Clustering value)
            {
                while (idx >= 0)
                {
                    if (!inSlice)
                    {
                        int cmp = comparator.compare(slices[idx].end(), value);
                        // value > end
                        if (cmp > 0)
                            return false;

                        inSlice = true;

                        if (cmp == 0)
                            return true;
                    }

                    // Here, value <= end and inSlice
                    if (comparator.compare(slices[idx].start(), value) <= 0)
                        return true;

                    --idx;
                    inSlice = false;
                }
                return false;
            }

            public boolean isDone()
            {
                return idx < 0;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < slices.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(slices[i].toString(comparator));
            }
            return sb.append("}").toString();
        }

        public String toCQLString(CFMetaData metadata)
        {
            StringBuilder sb = new StringBuilder();

            // In CQL, condition are expressed by column, so first group things that way,
            // i.e. for each column, we create a list of what each slice contains on that column
            int clusteringSize = metadata.clusteringColumns().size();
            List<List<ComponentOfSlice>> columnComponents = new ArrayList<>(clusteringSize);
            for (int i = 0; i < clusteringSize; i++)
            {
                List<ComponentOfSlice> perSlice = new ArrayList<>();
                columnComponents.add(perSlice);

                for (int j = 0; j < slices.length; j++)
                {
                    ComponentOfSlice c = ComponentOfSlice.fromSlice(i, slices[j]);
                    if (c != null)
                        perSlice.add(c);
                }
            }

            boolean needAnd = false;
            for (int i = 0; i < clusteringSize; i++)
            {
                ColumnDefinition column = metadata.clusteringColumns().get(i);
                List<ComponentOfSlice> componentInfo = columnComponents.get(i);
                if (componentInfo.isEmpty())
                    break;

                // For a given column, there is only 3 cases that CQL currently generates:
                //   1) every slice are EQ with the same value, it's a simple '=' relation.
                //   2) every slice are EQ but with different values, it's a IN relation.
                //   3) every slice aren't EQ but have the same values, we have inequality relations.
                // Note that this doesn't cover everything that ReadCommand can express, but
                // as it's all that CQL support for now, we'll ignore other cases (which would then
                // display a bogus query but that's not the end of the world).
                // TODO: we should improve this at some point.
                ComponentOfSlice first = componentInfo.get(0);
                if (first.isEQ())
                {
                    if (needAnd)
                        sb.append(" AND ");
                    needAnd = true;

                    sb.append(column.name);

                    Set<ByteBuffer> values = new LinkedHashSet<>();
                    for (int j = 0; j < componentInfo.size(); j++)
                        values.add(componentInfo.get(j).startValue);

                    if (values.size() == 1)
                    {
                        sb.append(" = ").append(column.type.getString(first.startValue));
                    }
                    else
                    {
                        sb.append(" IN (");
                        int j = 0;
                        for (ByteBuffer value : values)
                            sb.append(j++ == 0 ? "" : ", ").append(column.type.getString(value));
                        sb.append(")");
                    }
                }
                else
                {
                    // As said above, we assume (without checking) that this means all ComponentOfSlice for this column
                    // are the same, so we only bother about the first.
                    if (first.startValue != null)
                    {
                        if (needAnd)
                            sb.append(" AND ");
                        needAnd = true;
                        sb.append(column.name).append(first.startInclusive ? " >= " : " > ").append(column.type.getString(first.startValue));
                    }
                    if (first.endValue != null)
                    {
                        if (needAnd)
                            sb.append(" AND ");
                        needAnd = true;
                        sb.append(column.name).append(first.endInclusive ? " <= " : " < ").append(column.type.getString(first.endValue));
                    }
                }
            }
            return sb.toString();
        }

        // An somewhat adhoc utility class only used by toCQLString
        private static class ComponentOfSlice
        {
            public final boolean startInclusive;
            public final ByteBuffer startValue;
            public final boolean endInclusive;
            public final ByteBuffer endValue;

            private ComponentOfSlice(boolean startInclusive, ByteBuffer startValue, boolean endInclusive, ByteBuffer endValue)
            {
                this.startInclusive = startInclusive;
                this.startValue = startValue;
                this.endInclusive = endInclusive;
                this.endValue = endValue;
            }

            public static ComponentOfSlice fromSlice(int component, Slice slice)
            {
                Slice.Bound start = slice.start();
                Slice.Bound end = slice.end();

                if (component >= start.size() && component >= end.size())
                    return null;

                boolean startInclusive = true, endInclusive = true;
                ByteBuffer startValue = null, endValue = null;
                if (component < start.size())
                {
                    startInclusive = start.isInclusive();
                    startValue = start.get(component);
                }
                if (component < end.size())
                {
                    endInclusive = end.isInclusive();
                    endValue = end.get(component);
                }
                return new ComponentOfSlice(startInclusive, startValue, endInclusive, endValue);
            }

            public boolean isEQ()
            {
                return startValue.equals(endValue);
            }
        }
    }

    /**
     * Specialized implementation of {@code Slices} that selects all rows.
     * <p>
     * This is equivalent to having the single {@code Slice.ALL} slice, but is somewhat more effecient.
     */
    private static class SelectAllSlices extends Slices
    {
        private static final InOrderTester trivialTester = new InOrderTester()
        {
            public boolean includes(Clustering value)
            {
                return true;
            }

            public boolean isDone()
            {
                return false;
            }
        };

        public int size()
        {
            return 1;
        }

        public Slice get(int i)
        {
            return Slice.ALL;
        }

        public boolean hasLowerBound()
        {
            return false;
        }

        public boolean hasUpperBound()
        {
            return false;
        }

        public boolean selects(Clustering clustering)
        {
            return true;
        }

        public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean reversed)
        {
            return new ArrayBackedSlices(comparator, new Slice[]{ Slice.ALL.forPaging(comparator, lastReturned, reversed) });
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return trivialTester;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return true;
        }

        public AtomIterator makeSliceIterator(SliceableAtomIterator iter)
        {
            return iter;
        }

        public Iterator<Slice> iterator()
        {
            return Iterators.singletonIterator(Slice.ALL);
        }

        @Override
        public String toString()
        {
            return "ALL";
        }

        public String toCQLString(CFMetaData metadata)
        {
            return "";
        }
    }

    /**
     * Specialized implementation of {@code Slices} that selects no rows.
     */
    private static class SelectNoSlices extends Slices
    {
        private static final InOrderTester trivialTester = new InOrderTester()
        {
            public boolean includes(Clustering value)
            {
                return false;
            }

            public boolean isDone()
            {
                return true;
            }
        };

        public int size()
        {
            return 0;
        }

        public Slice get(int i)
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasLowerBound()
        {
            return false;
        }

        public boolean hasUpperBound()
        {
            return false;
        }

        public Slices forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean reversed)
        {
            return this;
        }

        public boolean selects(Clustering clustering)
        {
            return false;
        }

        public InOrderTester inOrderTester(boolean reversed)
        {
            return trivialTester;
        }

        public boolean intersects(List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return false;
        }

        public AtomIterator makeSliceIterator(SliceableAtomIterator iter)
        {
            return AtomIterators.emptyIterator(iter.metadata(), iter.partitionKey(), iter.isReverseOrder(), iter.nowInSec());
        }

        public Iterator<Slice> iterator()
        {
            return Iterators.emptyIterator();
        }

        @Override
        public String toString()
        {
            return "NONE";
        }

        public String toCQLString(CFMetaData metadata)
        {
            return "";
        }
    }
}
