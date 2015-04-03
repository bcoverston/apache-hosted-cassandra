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
package org.apache.cassandra.db.index.composites;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

import org.apache.cassandra.db.marshal.*;

public class CompositesSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(CompositesSearcher.class);

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        super(indexManager, columns);
    }

    public PartitionIterator search(ReadCommand command)
    {
        ColumnFilter.Expression primary = primaryClause(command);
        assert primary != null;

        CompositesIndex index = (CompositesIndex)indexManager.getIndexForColumn(primary.column());
        assert index != null && index.getIndexCfs() != null;

        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}", primary);

        DecoratedKey indexKey = index.getIndexKeyFor(primary.getIndexValue());

        // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
        // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
        final OpOrder.Group writeOp = baseCfs.keyspace.writeOrder.start();
        final OpOrder.Group baseOp = baseCfs.readOrdering.start();
        final OpOrder.Group indexOp = index.getIndexCfs().readOrdering.start();
        try
        {
            AtomIterator indexIter = new WrappingAtomIterator(queryIndex(index, indexKey, command))
            {
                @Override
                public void close()
                {
                    try
                    {
                        super.close();
                    }
                    finally
                    {
                        indexOp.close();
                    }
                }
            };

            try
            {
                return new WrappingPartitionIterator(queryDataFromIndex(index, indexKey, AtomIterators.asRowIterator(indexIter), command))
                {
                    @Override
                    public void close()
                    {
                        try
                        {
                            super.close();
                        }
                        finally
                        {
                            baseOp.close();
                            writeOp.close();
                        }
                    }
                };
            }
            catch (RuntimeException | Error e)
            {
                indexIter.close();
                throw e;
            }
        }
        catch (RuntimeException | Error e)
        {
            indexOp.close();
            baseOp.close();
            writeOp.close();
            throw e;
        }
    }

    public ColumnFilter.Expression primaryClause(ReadCommand command)
    {
        return highestSelectivityPredicate(command.columnFilter());
    }

    private AtomIterator queryIndex(CompositesIndex index, DecoratedKey indexKey, ReadCommand command)
    {
        PartitionFilter filter = makeIndexFilter(index, command);
        return SinglePartitionReadCommand.create(index.getIndexCfs().metadata, command.nowInSec(), indexKey, filter)
                                         .queryMemtableAndDisk(index.getIndexCfs());
    }

    private PartitionFilter makeIndexFilter(CompositesIndex index, ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand sprc = (SinglePartitionReadCommand)command;
            ByteBuffer pk = sprc.partitionKey().getKey();
            PartitionFilter filter = sprc.partitionFilter();

            if (filter instanceof NamesPartitionFilter)
            {
                SortedSet<Clustering> requested = ((NamesPartitionFilter)filter).requestedRows();
                SortedSet<Clustering> clusterings = new TreeSet<>(index.getIndexComparator());
                for (Clustering c : requested)
                    clusterings.add(index.makeIndexClustering(pk, c, null).takeAlias());
                return new NamesPartitionFilter(PartitionColumns.NONE, clusterings, filter.isReversed());
            }
            else
            {
                Slices requested = ((SlicePartitionFilter)filter).requestedSlices();
                Slices.Builder builder = new Slices.Builder(index.getIndexComparator());
                for (Slice slice : requested)
                    builder.add(index.makeIndexBound(pk, slice.start()), index.makeIndexBound(pk, slice.end()));
                return new SlicePartitionFilter(PartitionColumns.NONE, builder.build(), filter.isReversed());
            }
        }
        else
        {
            DataRange dataRange = ((PartitionRangeReadCommand)command).dataRange();
            AbstractBounds<RowPosition> range = dataRange.keyRange();

            Slice slice = Slice.ALL;

            /*
             * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
             * the indexed row unfortunately (which will be inefficient), because we have no way to intuit the smallest possible
             * key having a given token. A potential fix would be to actually store the token along the key in the indexed row.
             */
            if (range.left instanceof DecoratedKey)
            {
                assert range.right instanceof DecoratedKey;

                DecoratedKey startKey = (DecoratedKey)range.left;
                DecoratedKey endKey = (DecoratedKey)range.right;

                Slice.Bound start = Slice.Bound.BOTTOM;
                Slice.Bound end = Slice.Bound.TOP;

                /*
                 * For index queries over a range, we can't do a whole lot better than querying everything for the key range, though for
                 * slice queries where we can slightly restrict the beginning and end.
                 */
                if (!dataRange.isNamesQuery())
                {
                    SlicePartitionFilter startSliceFilter = ((SlicePartitionFilter)dataRange.partitionFilter(startKey));
                    SlicePartitionFilter endSliceFilter = ((SlicePartitionFilter)dataRange.partitionFilter(endKey));

                    // We can't effectively support reversed queries when we have a range, so we don't support it
                    // (or through post-query reordering) and shouldn't get there.
                    assert !startSliceFilter.isReversed() && !endSliceFilter.isReversed();

                    Slices startSlices = startSliceFilter.requestedSlices();
                    Slices endSlices = endSliceFilter.requestedSlices();

                    if (startSlices.size() > 0)
                        start = startSlices.get(0).start();

                    if (endSlices.size() > 0)
                        end = endSlices.get(endSlices.size() - 1).end();
                }

                slice = Slice.make(index.makeIndexBound(startKey.getKey(), start), index.makeIndexBound(endKey.getKey(), end));
            }

            return new SlicePartitionFilter(PartitionColumns.NONE, Slices.with(index.getIndexComparator(), slice), false);
        }
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, CompositesIndex.IndexedEntry entry, ReadCommand command)
    {
        return command.selects(partitionKey, entry.indexedEntryClustering);
    }

    private PartitionIterator queryDataFromIndex(final CompositesIndex index, final DecoratedKey indexKey, final RowIterator indexHits, final ReadCommand command)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        return new PartitionIterator()
        {
            private CompositesIndex.IndexedEntry nextEntry;

            private AtomIterator next;

            public boolean hasNext()
            {
                return prepareNext();
            }

            public AtomIterator next()
            {
                if (next == null)
                    prepareNext();

                AtomIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                if (next != null)
                    return true;

                if (nextEntry == null)
                {
                    if (!indexHits.hasNext())
                        return false;

                    nextEntry = index.decodeEntry(indexKey, indexHits.next());
                }


                // Gather all index hits belonging to the same partition and query the data for those hits.
                // TODO: it's much more efficient to do 1 read for all hits to the same partition than doing
                // 1 read per index hit. However, this basically mean materializing all hits for a partition
                // in memory so we should consider adding some paging mechanism. However, index hits should
                // be relatively small so it's much betterthat the previous  code that was materializing all
                // *data* for a given partition.
                SortedSet<Clustering> clusterings = new TreeSet<>(baseCfs.getComparator());
                DecoratedKey partitionKey = baseCfs.partitioner.decorateKey(nextEntry.indexedKey);

                while (nextEntry != null && partitionKey.getKey().equals(nextEntry.indexedKey))
                {
                    // We're queried a slice of the index, but some hits may not match some of the clustering column constraints
                    if (isMatchingEntry(partitionKey, nextEntry, command))
                        clusterings.add(nextEntry.indexedEntryClustering.takeAlias());

                    nextEntry = indexHits.hasNext() ? index.decodeEntry(indexKey, indexHits.next()) : null;
                }

                // Because we've eliminated entries that don't match the clustering columns, it's possible we added nothing
                if (clusterings.isEmpty())
                    return prepareNext();

                // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                NamesPartitionFilter filter = new NamesPartitionFilter(command.queriedColumns(), clusterings, false);
                SinglePartitionReadCommand dataCmd = new SinglePartitionNamesCommand(baseCfs.metadata,
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     DataLimits.NONE,
                                                                                     partitionKey,
                                                                                     filter);
                AtomIterator dataIter = filterStaleEntries(dataCmd.queryMemtableAndDisk(baseCfs), index, indexKey.getKey());
                if (AtomIterators.isEmpty(dataIter))
                {
                    dataIter.close();
                    return prepareNext();
                }

                next = dataIter;
                return true;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                indexHits.close();
                if (next != null)
                    next.close();
            }
        };
    }

    private AtomIterator filterStaleEntries(AtomIterator dataIter, final CompositesIndex index, final ByteBuffer indexValue)
    {
        return new WrappingAtomIterator(dataIter)
        {
            private Atom next;

            @Override
            public boolean hasNext()
            {
                return prepareNext();
            }

            @Override
            public Atom next()
            {
                if (next == null)
                    prepareNext();

                Atom toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                if (next != null)
                    return true;

                while (next == null && super.hasNext())
                {
                    next = super.next();
                    if (next.kind() != Atom.Kind.ROW || !index.isStale((Row)next, indexValue))
                        return true;

                    next = null;
                }
                return false;
            }
        };
    }
}
