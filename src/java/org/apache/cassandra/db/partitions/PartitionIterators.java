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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static methods to work with partition iterators.
 */
public abstract class PartitionIterators
{
    private static final Serializer serializer = new Serializer();

    public static final PartitionIterator EMPTY = new AbstractPartitionIterator()
    {
        public boolean hasNext()
        {
            return false;
        }

        public AtomIterator next()
        {
            throw new NoSuchElementException();
        }
    };

    private static final Comparator<AtomIterator> partitionComparator = new Comparator<AtomIterator>()
    {
        public int compare(AtomIterator p1, AtomIterator p2)
        {
            return p1.partitionKey().compareTo(p2.partitionKey());
        }
    };

    private PartitionIterators() {}

    public interface MergeListener
    {
        public AtomIterators.MergeListener getAtomMergeListener(DecoratedKey partitionKey, List<AtomIterator> versions);
        public void close();
    }

    public static AtomIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        AtomIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : AtomIterators.emptyIterator(command.metadata(),
                                                            command.partitionKey(),
                                                            command.partitionFilter().isReversed(),
                                                            command.nowInSec());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole PartitionIterator.
        return new WrappingAtomIterator(toReturn)
        {
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    // asserting this only now because it bothers Serializer if hasNext() is called before
                    // the previously returned iterator hasn't been fully consumed.
                    assert !iter.hasNext();

                    iter.close();
                }
            }
        };
    }

    public static DataIterator mergeAsDataIterator(List<PartitionIterator> iterators, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the AtomIterators directly as RowIterators
        return asDataIterator(merge(iterators, listener));
    }

    public static DataIterator asDataIterator(final PartitionIterator iterator)
    {
        return new DataIterator()
        {
            private RowIterator next;

            public boolean hasNext()
            {
                while (next == null && iterator.hasNext())
                {
                    AtomIterator atomIter = iterator.next();
                    next = new RowIteratorFromAtomIterator(atomIter);
                    if (RowIterators.isEmpty(next))
                    {
                        atomIter.close();
                        next = null;
                    }
                }
                return next != null;
            }

            public RowIterator next()
            {
                if (next == null && !hasNext())
                    throw new NoSuchElementException();

                RowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                try
                {
                    iterator.close();
                }
                finally
                {
                    if (next != null)
                        next.close();
                }
            }
        };
    }

    public static PartitionIterator merge(final List<? extends PartitionIterator> iterators, final MergeListener listener)
    {
        assert listener != null;

        final MergeIterator<AtomIterator, AtomIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<AtomIterator, AtomIterator>()
        {
            private final List<AtomIterator> toMerge = new ArrayList<>(iterators.size());

            private CFMetaData metadata;
            private DecoratedKey partitionKey;
            private boolean isReverseOrder;
            private int nowInSec;

            public void reduce(int idx, AtomIterator current)
            {
                metadata = current.metadata();
                partitionKey = current.partitionKey();
                isReverseOrder = current.isReverseOrder();
                nowInSec = current.nowInSec();

                // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                // Non-present iterator will thus be set to empty in getReduced.
                toMerge.set(idx, current);
            }

            protected AtomIterator getReduced()
            {
                // Replace nulls by empty iterators
                AtomIterators.MergeListener atomListener = listener.getAtomMergeListener(partitionKey, toMerge);

                for (int i = 0; i < toMerge.size(); i++)
                    if (toMerge.get(i) == null)
                        toMerge.set(i, AtomIterators.emptyIterator(metadata, partitionKey, isReverseOrder, nowInSec));

                return AtomIterators.merge(toMerge, atomListener);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                for (int i = 0; i < iterators.size(); i++)
                    toMerge.add(null);
            }
        });

        return new AbstractPartitionIterator()
        {
            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public AtomIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static PartitionIterator mergeLazily(final List<? extends PartitionIterator> iterators)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        final MergeIterator<AtomIterator, AtomIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<AtomIterator, AtomIterator>()
        {
            private final List<AtomIterator> toMerge = new ArrayList<>(iterators.size());

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return false;
            }

            public void reduce(int idx, AtomIterator current)
            {
                toMerge.add(current);
            }

            protected AtomIterator getReduced()
            {
                return new LazilyInitializedAtomIterator(toMerge.get(0).partitionKey())
                {
                    protected AtomIterator initializeIterator()
                    {
                        return AtomIterators.merge(toMerge);
                    }
                };
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });

        return new AbstractPartitionIterator()
        {
            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public AtomIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    public static PartitionIterator removeDroppedColumns(PartitionIterator iterator, final Map<ColumnIdentifier, CFMetaData.DroppedColumn> droppedColumns)
    {
        FilteringRow filter = new FilteringRow()
        {
            @Override
            protected boolean include(Cell cell)
            {
                return include(cell.column(), cell.livenessInfo().timestamp());
            }

            @Override
            protected boolean include(ColumnDefinition c, DeletionTime dt)
            {
                return include(c, dt.markedForDeleteAt());
            }

            private boolean include(ColumnDefinition column, long timestamp)
            {
                CFMetaData.DroppedColumn dropped = droppedColumns.get(column.name);
                return dropped == null || timestamp > dropped.droppedTime;
            }
        };

        return new AbstractFilteringIterator(iterator, filter)
        {
            protected boolean shouldFilter(AtomIterator atoms)
            {
                // TODO: We could have atom iterators return the smallest timestamp they might return
                // (which we can get from sstable stats), and ignore any dropping if that smallest
                // timestamp is bigger that the biggest droppedColumns timestamp.

                // If none of the dropped columns is part of the columns that the iterator actually returns, there is nothing to do;
                for (ColumnDefinition c : atoms.columns())
                    if (droppedColumns.containsKey(c.name))
                        return true;

                return false;
            }
        };
    }

    public static void digest(PartitionIterator iterator, MessageDigest digest)
    {
        try (PartitionIterator iter = iterator)
        {
            while (iter.hasNext())
            {
                try (AtomIterator partition = iter.next())
                {
                    AtomIterators.digest(partition, digest);
                }
            }
        }
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    /**
     * Wraps the provided iterator so it logs the returned atoms for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static PartitionIterator loggingIterator(PartitionIterator iterator, final String id, final boolean fullDetails)
    {
        return new WrappingPartitionIterator(iterator)
        {
            public AtomIterator next()
            {
                return AtomIterators.loggingIterator(super.next(), id, fullDetails);
            }
        };
    }

    /**
     * Serialize each AtomSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public void serialize(PartitionIterator iter, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (AtomIterator partition = iter.next())
                {
                    AtomIteratorSerializer.serializer.serialize(partition, out, version);
                }
            }
            out.writeBoolean(false);
        }

        public PartitionIterator deserialize(final DataInput in, final int version, final LegacyLayout.Flag flag) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            return new AbstractPartitionIterator()
            {
                private AtomIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public AtomIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = AtomIteratorSerializer.serializer.deserialize(in, version, flag);
                        return next;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }
    }
}
