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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.service.StorageService;
import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public class AtomicBTreePartition implements Partition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartition.class);

    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition((CFMetaData.denseCFMetaData("keyspace", "table", BytesType.instance)),
                                                                                       StorageService.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
                                                                                       null));

    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");

    private static final DeletionInfo LIVE = DeletionInfo.live();
    // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
    // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE, null, AtomStats.NO_STATS);

    private final CFMetaData metadata;
    private final DecoratedKey partitionKey;
    private final MemtableAllocator allocator;

    private volatile Holder ref;

    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, Holder.class, "ref");

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.allocator = allocator;
        this.ref = EMPTY;
    }

    public boolean isEmpty()
    {
        return ref.deletionInfo.isLive() && BTree.isEmpty(ref.tree);
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return ref.deletionInfo.getPartitionDeletion();
    }

    public PartitionColumns columns()
    {
        // We don't really know which columns will be part of the update, so assume it's all of them
        return metadata.partitionColumns();
    }

    public boolean hasRows()
    {
        return !BTree.isEmpty(ref.tree);
    }

    public AtomStats stats()
    {
        return ref.stats;
    }

    public SearchIterator<Clustering, Row> searchIterator(final PartitionColumns columns, final boolean reversed, final int nowInSec)
    {
        // TODO: we could optimize comparison for "NativeRow" à la #6755
        final Holder current = ref;
        return new SearchIterator<Clustering, Row>()
        {
            private final SearchIterator<Clustering, MemtableRowData> rawIter = new BTreeSearchIterator<>(current.tree, metadata.comparator, !reversed);
            private final MemtableRowData.ReusableRow row = allocator.newReusableRow(nowInSec);
            private final ReusableFilteringRow filter = new ReusableFilteringRow(columns.regulars);
            private final long partitionDeletion = current.deletionInfo.getPartitionDeletion().markedForDeleteAt();

            public boolean hasNext()
            {
                return rawIter.hasNext();
            }

            public Row next(Clustering key)
            {
                if (key == Clustering.STATIC_CLUSTERING)
                    return makeStatic(columns, current, allocator, nowInSec);

                MemtableRowData data = rawIter.next(key);
                if (data == null)
                    return null;

                // We also need to find if there is a range tombstone covery this key
                RangeTombstone rt = current.deletionInfo.rangeCovering(key);
                if (rt == null || rt.deletionTime().markedForDeleteAt() < partitionDeletion)
                    filter.setDeletionTimestamp(partitionDeletion);
                else
                    filter.setDeletionTimestamp(rt.deletionTime().markedForDeleteAt());

                return filter.setTo(row.setTo(data));
            }
        };
    }

    public AtomIterator atomIterator(PartitionColumns columns, Slices slices, boolean reversed, int nowInSec)
    {
        if (slices.size() == 0)
        {
            Holder current = ref;
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            if (columns.statics.isEmpty() && partitionDeletion.isLive())
                return AtomIterators.emptyIterator(metadata, partitionKey, reversed, nowInSec);

            return new AbstractAtomIterator(metadata,
                                            partitionKey,
                                            partitionDeletion,
                                            columns,
                                            makeStatic(columns, current, allocator, nowInSec),
                                            reversed,
                                            current.stats,
                                            nowInSec)
            {
                protected Atom computeNext()
                {
                    return endOfData();
                }
            };
        }

        return slices.size() == 1
             ? new SingleSliceIterator(metadata, partitionKey, ref, columns, slices.get(0), reversed, allocator, nowInSec)
             : new SlicesIterator(metadata, partitionKey, ref, columns, slices, reversed, allocator, nowInSec);
    }

    private static Row makeStatic(PartitionColumns columns, Holder holder, MemtableAllocator allocator, int nowInSec)
    {
        if (columns.statics.isEmpty() || holder.staticRow == null)
            return Rows.EMPTY_STATIC_ROW;

        return new ReusableFilteringRow(columns.statics)
               .setDeletionTimestamp(holder.deletionInfo.getPartitionDeletion().markedForDeleteAt())
               .setTo(allocator.newReusableRow(nowInSec).setTo(holder.staticRow));
    }

    private static class ReusableFilteringRow extends FilteringRow
    {
        private final Columns columns;
        private long deletionTimestamp;

        public ReusableFilteringRow(Columns columns)
        {
            this.columns = columns;
        }

        public ReusableFilteringRow setDeletionTimestamp(long timestamp)
        {
            this.deletionTimestamp = timestamp;
            return this;
        }

        @Override
        protected boolean include(LivenessInfo info)
        {
            return info.timestamp() > deletionTimestamp;
        }

        @Override
        public long maxLiveTimestamp()
        {
            long maxLive = super.maxLiveTimestamp();
            return maxLive > deletionTimestamp ? maxLive : LivenessInfo.NO_TIMESTAMP;
        }

        @Override
        protected boolean include(ColumnDefinition def)
        {
            return columns.contains(def);
        }

        @Override
        protected boolean include(DeletionTime dt)
        {
            return dt.markedForDeleteAt() > deletionTimestamp;
        }

        @Override
        protected boolean include(ColumnDefinition c, DeletionTime dt)
        {
            return dt.markedForDeleteAt() > deletionTimestamp;
        }
    }

    private static class SingleSliceIterator extends AbstractAtomIterator
    {
        private final Iterator<Atom> atomIter;
        private final ReusableFilteringRow row;

        private SingleSliceIterator(CFMetaData metadata,
                                    DecoratedKey key,
                                    Holder holder,
                                    PartitionColumns columns,
                                    Slice slice,
                                    boolean isReversed,
                                    MemtableAllocator allocator,
                                    int nowInSec)
        {
            super(metadata,
                  key,
                  holder.deletionInfo.getPartitionDeletion(),
                  columns,
                  makeStatic(columns, holder, allocator, nowInSec),
                  isReversed,
                  holder.stats,
                  nowInSec);

            Iterator<Row> rowIter = rowIter(metadata,
                                            holder,
                                            slice,
                                            !isReversed,
                                            allocator,
                                            nowInSec);

            this.atomIter = new RowAndTombstoneMergeIterator(metadata.comparator, isReversed)
                            .setTo(rowIter, holder.deletionInfo.rangeIterator(slice, isReversed));

            this.row = new ReusableFilteringRow(columns.regulars)
                       .setDeletionTimestamp(partitionLevelDeletion.markedForDeleteAt());
        }

        private Iterator<Row> rowIter(CFMetaData metadata,
                                      Holder holder,
                                      Slice slice,
                                      boolean forwards,
                                      final MemtableAllocator allocator,
                                      final int nowInSec)
        {
            Slice.Bound start = slice.start() == Slice.Bound.BOTTOM ? null : slice.start();
            Slice.Bound end = slice.end() == Slice.Bound.TOP ? null : slice.end();
            final Iterator<MemtableRowData> dataIter = BTree.slice(holder.tree, metadata.comparator, start, true, end, true, forwards);
            return new AbstractIterator<Row>()
            {
                private final MemtableRowData.ReusableRow row = allocator.newReusableRow(nowInSec);

                protected Row computeNext()
                {
                    return dataIter.hasNext() ? row.setTo(dataIter.next()) : endOfData();
                }
            };
        }

        protected Atom computeNext()
        {
            while (atomIter.hasNext())
            {
                Atom next = atomIter.next();
                if (next.kind() == Atom.Kind.ROW)
                {
                    row.setTo((Row)next);
                    if (!row.isEmpty())
                        return row;
                }
                else
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker)next;
                    if (marker.clustering().isStart())
                    {
                        if (marker.deletionTime().markedForDeleteAt() > row.deletionTimestamp)
                            row.setDeletionTimestamp(marker.deletionTime().markedForDeleteAt());
                    }
                    else
                    {
                        row.setDeletionTimestamp(partitionLevelDeletion().markedForDeleteAt());
                    }
                    return marker;
                }
            }
            return endOfData();
        }
    }

    public static class SlicesIterator extends AbstractAtomIterator
    {
        private final Holder holder;
        private final MemtableAllocator allocator;
        private final Slices slices;
        private final int nowInSec;

        private int idx;
        private AtomIterator currentSlice;

        private SlicesIterator(CFMetaData metadata,
                               DecoratedKey key,
                               Holder holder,
                               PartitionColumns columns,
                               Slices slices,
                               boolean isReversed,
                               MemtableAllocator allocator,
                               int nowInSec)
        {
            super(metadata, key, holder.deletionInfo.getPartitionDeletion(), columns, makeStatic(columns, holder, allocator, nowInSec), isReversed, holder.stats, nowInSec);
            this.holder = holder;
            this.allocator = allocator;
            this.slices = slices;
            this.nowInSec = nowInSec;
        }

        protected Atom computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                    currentSlice = new SingleSliceIterator(metadata,
                                                           partitionKey,
                                                           holder,
                                                           columns,
                                                           slices.get(sliceIdx),
                                                           isReverseOrder,
                                                           allocator,
                                                           nowInSec);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time detla between updates.
     */
    public long[] addAllWithSizeDelta(final PartitionUpdate update, OpOrder.Group writeOp, Updater indexer)
    {
        RowUpdater updater = new RowUpdater(this, allocator, writeOp, indexer);
        DeletionInfo inputDeletionInfoCopy = null;

        boolean monitorOwned = false;
        try
        {
            if (usePessimisticLocking())
            {
                Locks.monitorEnterUnsafe(this);
                monitorOwned = true;
            }
            while (true)
            {
                Holder current = ref;
                updater.ref = current;
                updater.reset();

                DeletionInfo deletionInfo;
                if (update.deletionInfo().mayModify(current.deletionInfo))
                {
                    if (inputDeletionInfoCopy == null)
                        inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);

                    deletionInfo = current.deletionInfo.copy().add(inputDeletionInfoCopy);
                    updater.allocated(deletionInfo.unsharedHeapSize() - current.deletionInfo.unsharedHeapSize());
                }
                else
                {
                    deletionInfo = current.deletionInfo;
                }

                Row newStatic = update.staticRow();
                MemtableRowData staticRow = newStatic == Rows.EMPTY_STATIC_ROW
                                          ? current.staticRow
                                          : (current.staticRow == null ? updater.apply(newStatic) : updater.apply(current.staticRow, newStatic));
                Object[] tree = BTree.<Clusterable, Row, MemtableRowData>update(current.tree, update.metadata().comparator, update, update.rowCount(), updater);
                AtomStats newStats = current.stats.mergeWith(update.stats());

                if (tree != null && refUpdater.compareAndSet(this, current, new Holder(tree, deletionInfo, staticRow, newStats)))
                {
                    indexer.updateRowLevelIndexes();
                    updater.finish();
                    return new long[]{ updater.dataSize, updater.colUpdateTimeDelta };
                }
                else if (!monitorOwned)
                {
                    boolean shouldLock = usePessimisticLocking();
                    if (!shouldLock)
                    {
                        shouldLock = updateWastedAllocationTracker(updater.heapSize);
                    }
                    if (shouldLock)
                    {
                        Locks.monitorEnterUnsafe(this);
                        monitorOwned = true;
                    }
                }
            }
        }
        finally
        {
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }

    }

    public boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes)
    {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (System.nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }

    private static final class Holder
    {
        final DeletionInfo deletionInfo;
        // the btree of rows
        final Object[] tree;
        final MemtableRowData staticRow;
        final AtomStats stats;

        Holder(Object[] tree, DeletionInfo deletionInfo, MemtableRowData staticRow, AtomStats stats)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow;
            this.stats = stats;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info, this.staticRow, this.stats);
        }
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<Row, MemtableRowData>
    {
        final AtomicBTreePartition updating;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final Updater indexer;
        final int nowInSec;
        Holder ref;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        final MemtableRowData.ReusableRow row;
        final MemtableAllocator.DataReclaimer reclaimer;
        final MemtableAllocator.RowAllocator rowAllocator;
        List<MemtableRowData> inserted; // TODO: replace with walk of aborted BTree

        private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.row = allocator.newReusableRow(nowInSec);
            this.reclaimer = allocator.reclaimer();
            this.rowAllocator = allocator.newRowAllocator(updating.metadata(), writeOp);
        }

        public MemtableRowData apply(Row insert)
        {
            rowAllocator.allocateNewRow(insert.clustering().size(), insert.columns(), insert.isStatic());
            insert.copyTo(rowAllocator);
            MemtableRowData data = rowAllocator.allocatedRowData();

            insertIntoIndexes(insert);

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(data);
            return data;
        }

        public MemtableRowData apply(MemtableRowData existing, Row update)
        {
            Columns mergedColumns = existing.columns().mergeTo(update.columns());
            rowAllocator.allocateNewRow(update.clustering().size(), mergedColumns, update.isStatic());

            colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(row.setTo(existing), update, mergedColumns, rowAllocator, nowInSec, indexer));

            MemtableRowData reconciled = rowAllocator.allocatedRowData();

            dataSize += reconciled.dataSize() - existing.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(reconciled);
            discard(existing);

            return reconciled;
        }

        private void insertIntoIndexes(Row toInsert)
        {
            if (indexer == SecondaryIndexManager.nullUpdater)
                return;

            Clustering clustering = toInsert.clustering();
            indexer.maybeIndex(clustering, toInsert.maxLiveTimestamp(), toInsert.partitionKeyLivenessInfo().ttl(), toInsert.deletion());
            for (Cell cell : toInsert)
                indexer.insert(clustering, cell);
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
            {
                for (MemtableRowData row : inserted)
                    abort(row);
                inserted.clear();
            }
            reclaimer.cancel();
        }

        protected void abort(MemtableRowData abort)
        {
            reclaimer.reclaimImmediately(abort);
        }

        protected void discard(MemtableRowData discard)
        {
            reclaimer.reclaim(discard);
        }

        public boolean abortEarly()
        {
            return updating.ref != ref;
        }

        public void allocated(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void finish()
        {
            allocator.onHeap().allocate(heapSize, writeOp);
            reclaimer.commit();
        }
    }
}
