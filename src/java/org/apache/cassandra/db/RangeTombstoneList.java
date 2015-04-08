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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones [0, 10]@t1 and [5, 15]@t2, then if t2 > t1 (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>, IMeasurableMemory
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneList.class);

    private static long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList(null, 0));

    private final ClusteringComparator comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private Slice.Bound[] starts;
    private Slice.Bound[] ends;
    private long[] markedAts;
    private int[] delTimes;

    private long boundaryHeapSize;
    private int size;

    private RangeTombstoneList(ClusteringComparator comparator, Slice.Bound[] starts, Slice.Bound[] ends, long[] markedAts, int[] delTimes, long boundaryHeapSize, int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimes = delTimes;
        this.size = size;
        this.boundaryHeapSize = boundaryHeapSize;
    }

    public RangeTombstoneList(ClusteringComparator comparator, int capacity)
    {
        this(comparator, new Slice.Bound[capacity], new Slice.Bound[capacity], new long[capacity], new int[capacity], 0, 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public ClusteringComparator comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        return new RangeTombstoneList(comparator,
                                      Arrays.copyOf(starts, size),
                                      Arrays.copyOf(ends, size),
                                      Arrays.copyOf(markedAts, size),
                                      Arrays.copyOf(delTimes, size),
                                      boundaryHeapSize, size);
    }

    public RangeTombstoneList copy(AbstractAllocator allocator)
    {
        RangeTombstoneList copy =  new RangeTombstoneList(comparator,
                                                          new Slice.Bound[size],
                                                          new Slice.Bound[size],
                                                          Arrays.copyOf(markedAts, size),
                                                          Arrays.copyOf(delTimes, size),
                                                          boundaryHeapSize, size);


        for (int i = 0; i < size; i++)
        {
            copy.starts[i] = MemtableRowData.BufferSliceBound.clone(starts[i], allocator);
            copy.ends[i] = MemtableRowData.BufferSliceBound.clone(ends[i], allocator);
        }

        return copy;
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.deletedSlice().start(),
            tombstone.deletedSlice().end(),
            tombstone.deletionTime().markedForDeleteAt(),
            tombstone.deletionTime().localDeletionTime());
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case), 
     * but it doesn't assume it.
     */
    public void add(Slice.Bound start, Slice.Bound end, long markedAt, int delTime)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTime);
            return;
        }

        int c = comparator.compare(ends[size-1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTime);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos : -pos-1), start, end, markedAt, delTime);
        }
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size > 10 * tombstones.size)
        {
            for (int i = 0; i < tombstones.size; i++)
                add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.starts[j], ends[i]) < 0)
                {
                    insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clustering clustering, Cell cell)
    {
        int idx = searchInternal(clustering, 0, size);
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        return idx >= 0 && (cell.isCounterCell() || markedAts[idx] >= cell.livenessInfo().timestamp());
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime searchDeletionTime(Clustering name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : new SimpleDeletionTime(markedAts[idx], delTimes[idx]);
    }

    public RangeTombstone search(Clustering name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : rangeTombstone(idx);
    }

    /*
     * Return is the index of the range covering name if name is covered. If the return idx is negative,
     * no range cover name and -idx-1 is the index of the first range whose start is greater than name.
     */
    private int searchInternal(ClusteringPrefix name, int startIdx, int endIdx)
    {
        if (isEmpty())
            return -1;

        int pos = Arrays.binarySearch(starts, startIdx, endIdx, name, comparator);
        if (pos >= 0)
        {
            return pos;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, ends[idx]) <= 0 ? idx : -idx-2;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.NATIVE.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += starts[i].dataSize() + ends[i].dataSize();
            dataSize += TypeSizes.NATIVE.sizeof(markedAts[i]);
            dataSize += TypeSizes.NATIVE.sizeof(delTimes[i]);
        }
        return dataSize;
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    private RangeTombstone rangeTombstone(int idx)
    {
        return new RangeTombstone(Slice.make(starts[idx], ends[idx]), new SimpleDeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewStart(int idx, Slice.Bound newStart)
    {
        return new RangeTombstone(Slice.make(newStart, ends[idx]), new SimpleDeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewEnd(int idx, Slice.Bound newEnd)
    {
        return new RangeTombstone(Slice.make(starts[idx], newEnd), new SimpleDeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewBounds(int idx, Slice.Bound newStart, Slice.Bound newEnd)
    {
        return new RangeTombstone(Slice.make(newStart, newEnd), new SimpleDeletionTime(markedAts[idx], delTimes[idx]));
    }

    public Iterator<RangeTombstone> iterator()
    {
        return iterator(false);
    }

    public Iterator<RangeTombstone> iterator(boolean reversed)
    {
        return reversed
             ? new AbstractIterator<RangeTombstone>()
             {
                 private int idx = size - 1;

                 protected RangeTombstone computeNext()
                 {
                     if (idx < 0)
                         return endOfData();

                     return rangeTombstone(idx--);
                 }
             }
             : new AbstractIterator<RangeTombstone>()
             {
                 private int idx;

                 protected RangeTombstone computeNext()
                 {
                     if (idx >= size)
                         return endOfData();

                     return rangeTombstone(idx++);
                 }
             };
    }

    public Iterator<RangeTombstone> iterator(final Slice slice, boolean reversed)
    {
        return reversed ? reverseIterator(slice) : forwardIterator(slice);
    }

    private Iterator<RangeTombstone> forwardIterator(final Slice slice)
    {
        int startIdx = slice.start() == Slice.Bound.BOTTOM ? 0 : searchInternal(slice.start(), 0, size);
        final int start = startIdx < 0 ? -startIdx-1 : startIdx;

        if (start >= size)
            return Iterators.<RangeTombstone>emptyIterator();

        int finishIdx = slice.end() == Slice.Bound.TOP ? size - 1 : searchInternal(slice.end(), start, size);
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-2 : finishIdx;

        if (start > finish)
            return Iterators.<RangeTombstone>emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterating over successive slices.
            Slice.Bound s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            Slice.Bound e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx >= size || idx > finish)
                    return endOfData();

                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterating over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                if (idx == start && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx++, slice.start());
                if (idx == finish && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx++, slice.end());
                return rangeTombstone(idx++);
            }
        };
    }

    private Iterator<RangeTombstone> reverseIterator(final Slice slice)
    {
        int startIdx = slice.end() == Slice.Bound.TOP ? 0 : searchInternal(slice.end(), 0, size);
        // if startIdx is the first range after 'slice.end()' we care only until the previous range
        final int start = startIdx < 0 ? -startIdx-2 : startIdx;

        if (start >= size)
            return Iterators.<RangeTombstone>emptyIterator();

        int finishIdx = slice.start() == Slice.Bound.BOTTOM ? 0 : searchInternal(slice.start(), 0, start);
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-1 : finishIdx;

        if (start < finish)
            return Iterators.<RangeTombstone>emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterator over successive slices.
            Slice.Bound s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            Slice.Bound e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx < 0 || idx < finish)
                    return endOfData();

                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterator over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                if (idx == start && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx--, slice.end());
                if (idx == finish && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx--, slice.start());
                return rangeTombstone(idx++);
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size != that.size)
            return false;
        
        for (int i = 0; i < size; i++)
        {
            if (!starts[i].equals(that.starts[i]))
                return false;
            if (!ends[i].equals(that.ends[i]))
                return false;
            if (markedAts[i] != that.markedAts[i])
                return false;
            if (delTimes[i] != that.delTimes[i])
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        for (int i = 0; i < size; i++)
        {
            result += starts[i].hashCode() + ends[i].hashCode();
            result += (int)(markedAts[i] ^ (markedAts[i] >>> 32));
            result += delTimes[i];
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
        System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
        System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
        System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
        dst.size = src.size;
        dst.boundaryHeapSize = src.boundaryHeapSize;
    }

    /*
     * Inserts a new element starting at index i. This method assumes that:
     *    ends[i-1] < start < ends[i]
     * (note that we cannot have start == end since both will at least have a different bound "kind")
     *
     * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
     *   - s_i <= e_i
     *   - e_i < s_i+1
     * Basically, range are non overlapping and in order.
     */
    private void insertFrom(int i, Slice.Bound start, Slice.Bound end, long markedAt, int delTime)
    {
        while (i < size)
        {
            assert start.isStart() && end.isEnd();
            assert i == 0 || comparator.compare(ends[i-1], start) < 0;
            assert comparator.compare(start, ends[i]) < 0;

            if (Slice.isEmpty(comparator, start, end))
                return;

            // Do we overwrite the current element?
            if (markedAt > markedAts[i])
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(starts[i], start) < 0)
                {
                    Slice.Bound newEnd = start.invert();
                    if (!Slice.isEmpty(comparator, starts[i], newEnd))
                    {
                        addInternal(i, starts[i], start.invert(), markedAts[i], delTimes[i]);
                        i++;
                        setInternal(i, start, ends[i], markedAt, delTime);
                    }
                }

                // now, start <= starts[i]

                // Does the new element stops before the current one,
                int endCmp = comparator.compare(end, starts[i]);
                if (endCmp < 0)
                {
                    // Here start <= starts[i] and end < starts[i]
                    // This means the current element is before the current one. However, one special
                    addInternal(i, start, end, markedAt, delTime);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(ends[i], end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue
                    // on with the next element (with the new inserted start == current end).

                    // If we're on the last element, we can optimize
                    if (i == size-1)
                    {
                        setInternal(i, start, end, markedAt, delTime);
                        return;
                    }

                    // Otherwise, the new element overwite until the min(end, next start)
                    if (comparator.compare(end, starts[i+1]) < 0)
                    {
                        setInternal(i, start, end, markedAt, delTime);
                        // We have fully handled the new element so we're done
                        return;
                    }

                    setInternal(i, start, starts[i+1].invert(), markedAt, delTime);
                    start = starts[i+1];
                    i++;
                }
                else
                {
                    // We don't overwrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    addInternal(i, start, end, markedAt, delTime);
                    i++;
                    Slice.Bound newStart = end.invert();
                    if (!Slice.isEmpty(comparator, newStart, ends[i]))
                        setInternal(i, newStart, ends[i], markedAts[i], delTimes[i]);
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, starts[i]) < 0)
                {
                    // If we stop before the start of the current element, just insert the new
                    // interval and we're done; otherwise insert until the beginning of the
                    // current element
                    if (comparator.compare(end, starts[i]) < 0)
                    {
                        addInternal(i, start, end, markedAt, delTime);
                        return;
                    }
                    Slice.Bound newEnd = starts[i].invert();
                    if (!Slice.isEmpty(comparator, start, newEnd))
                    {
                        addInternal(i, start, newEnd, markedAt, delTime);
                        i++;
                    }
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, ends[i]) <= 0)
                    return;

                start = ends[i].invert();
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        addInternal(i, start, end, markedAt, delTime);
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, Slice.Bound start, Slice.Bound end, long markedAt, int delTime)
    {
        assert i >= 0;

        if (size == capacity())
            growToFree(i);
        else if (i < size)
            moveElements(i);

        setInternal(i, start, end, markedAt, delTime);
        size++;
    }

    private void removeInternal(int i)
    {
        assert i >= 0;

        System.arraycopy(starts, i+1, starts, i, size - i - 1);
        System.arraycopy(ends, i+1, ends, i, size - i - 1);
        System.arraycopy(markedAts, i+1, markedAts, i, size - i - 1);
        System.arraycopy(delTimes, i+1, delTimes, i, size - i - 1);

        --size;
        starts[size] = null;
        ends[size] = null;
    }

    /*
     * Grow the arrays, leaving index i "free" in the process.
     */
    private void growToFree(int i)
    {
        int newLength = (capacity() * 3) / 2 + 1;
        grow(i, newLength);
    }

    /*
     * Grow the arrays to match newLength capacity.
     */
    private void grow(int newLength)
    {
        if (capacity() < newLength)
            grow(-1, newLength);
    }

    private void grow(int i, int newLength)
    {
        starts = grow(starts, size, newLength, i);
        ends = grow(ends, size, newLength, i);
        markedAts = grow(markedAts, size, newLength, i);
        delTimes = grow(delTimes, size, newLength, i);
    }

    private static Slice.Bound[] grow(Slice.Bound[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        Slice.Bound[] newA = new Slice.Bound[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static long[] grow(long[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        long[] newA = new long[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static int[] grow(int[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        int[] newA = new int[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    /*
     * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
     */
    private void moveElements(int i)
    {
        if (i >= size)
            return;

        System.arraycopy(starts, i, starts, i+1, size - i);
        System.arraycopy(ends, i, ends, i+1, size - i);
        System.arraycopy(markedAts, i, markedAts, i+1, size - i);
        System.arraycopy(delTimes, i, delTimes, i+1, size - i);
        // we set starts[i] to null to indicate the position is now empty, so that we update boundaryHeapSize
        // when we set it
        starts[i] = null;
    }

    private void setInternal(int i, Slice.Bound start, Slice.Bound end, long markedAt, int delTime)
    {
        if (starts[i] != null)
            boundaryHeapSize -= starts[i].unsharedHeapSize() + ends[i].unsharedHeapSize();
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimes[i] = delTime;
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
                + boundaryHeapSize
                + ObjectSizes.sizeOfArray(starts)
                + ObjectSizes.sizeOfArray(ends)
                + ObjectSizes.sizeOfArray(markedAts)
                + ObjectSizes.sizeOfArray(delTimes);
    }

    // TODO: This should be moved someplace else as it shouldn't be used directly: some ranges might become
    // complex deletion times. We'll also only need this for backward compatibility, this isn't used otherwise.
    public static class Serializer implements IVersionedSerializer<RangeTombstoneList>
    {
        private final LegacyLayout layout;

        public Serializer(LegacyLayout layout)
        {
            this.layout = layout;
        }

        public void serialize(RangeTombstoneList tombstones, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //if (tombstones == null)
            //{
            //    out.writeInt(0);
            //    return;
            //}

            //out.writeInt(tombstones.size);
            //for (int i = 0; i < tombstones.size; i++)
            //{
            //    layout.serializer().serialize(tombstones.starts[i], out);
            //    layout.serializer().serialize(tombstones.ends[i], out);
            //    out.writeInt(tombstones.delTimes[i]);
            //    out.writeLong(tombstones.markedAts[i]);
            //}
        }

        public RangeTombstoneList deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();

            //int size = in.readInt();
            //if (size == 0)
            //    return null;

            //RangeTombstoneList tombstones = new RangeTombstoneList(layout, size);

            //for (int i = 0; i < size; i++)
            //{
            //    Slice.Bound start = layout.serializer().deserialize(in);
            //    Slice.Bound end = layout.serializer().deserialize(in);
            //    int delTime =  in.readInt();
            //    long markedAt = in.readLong();

            //    if (version >= MessagingService.VERSION_20)
            //    {
            //        tombstones.setInternal(i, start, end, markedAt, delTime);
            //    }
            //    else
            //    {
            //        /*
            //         * The old implementation used to have range sorted by left value, but with potentially
            //         * overlapping range. So we need to use the "slow" path.
            //         */
            //        tombstones.add(start, end, markedAt, delTime);
            //    }
            //}

            //// The "slow" path take care of updating the size, but not the fast one
            //if (version >= MessagingService.VERSION_20)
            //    tombstones.size = size;
            //return tombstones;
        }

        public long serializedSize(RangeTombstoneList tombstones, TypeSizes typeSizes, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
            //if (tombstones == null)
            //    return typeSizes.sizeof(0);

            //long size = typeSizes.sizeof(tombstones.size);
            //for (int i = 0; i < tombstones.size; i++)
            //{
            //    size += type.serializer().serializedSize(tombstones.starts[i], typeSizes);
            //    size += type.serializer().serializedSize(tombstones.ends[i], typeSizes);
            //    size += typeSizes.sizeof(tombstones.delTimes[i]);
            //    size += typeSizes.sizeof(tombstones.markedAts[i]);
            //}
            //return size;
        }

        public long serializedSize(RangeTombstoneList tombstones, int version)
        {
            return serializedSize(tombstones, TypeSizes.NATIVE, version);
        }
    }
}
