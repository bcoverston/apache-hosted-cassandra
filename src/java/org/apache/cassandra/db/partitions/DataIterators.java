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

import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.util.FileUtils;

public abstract class DataIterators
{
    private DataIterators() {}

    public static final DataIterator EMPTY = new DataIterator()
    {
        public boolean hasNext()
        {
            return false;
        }

        public RowIterator next()
        {
            throw new NoSuchElementException();
        }

        public void remove()
        {
        }

        public void close()
        {
        }
    };

    public static RowIterator getOnlyElement(final DataIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        RowIterator toReturn = iter.hasNext()
                             ? iter.next()
                             : RowIterators.emptyIterator(command.metadata(),
                                                          command.partitionKey(),
                                                          command.partitionFilter().isReversed(),
                                                          command.nowInSec());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole DataIterator.
        return new WrappingRowIterator(toReturn)
        {
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    // asserting this only now because it bothers PartitionIterators.Serializer (which might be used
                    // under the provided DataIter) if hasNext() is called before the previously returned iterator hasn't been fully consumed.
                    assert !iter.hasNext();

                    iter.close();
                }
            }
        };
    }

    public static DataIterator concat(final List<DataIterator> iterators)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        return new DataIterator()
        {
            private int idx = 0;

            public boolean hasNext()
            {
                while (idx < iterators.size())
                {
                    if (iterators.get(idx).hasNext())
                        return true;

                    ++idx;
                }
                return false;
            }

            public RowIterator next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();
                return iterators.get(idx).next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                FileUtils.closeQuietly(iterators);
            }
        };
    }

    public static void digest(DataIterator iterator, MessageDigest digest)
    {
        while (iterator.hasNext())
        {
            try (RowIterator partition = iterator.next())
            {
                RowIterators.digest(partition, digest);
            }
        }
    }

    public static DataIterator singletonIterator(RowIterator iterator)
    {
        return new SingletonDataIterator(iterator);
    }

    /**
     * Wraps the provided iterator so it logs the returned rows for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static DataIterator loggingIterator(DataIterator iterator, final String id)
    {
        return new WrappingDataIterator(iterator)
        {
            public RowIterator next()
            {
                return RowIterators.loggingIterator(super.next(), id);
            }
        };
    }

    private static class SingletonDataIterator extends AbstractIterator<RowIterator> implements DataIterator
    {
        private final RowIterator iterator;
        private boolean returned;

        private SingletonDataIterator(RowIterator iterator)
        {
            this.iterator = iterator;
        }

        protected RowIterator computeNext()
        {
            if (returned)
                return endOfData();

            returned = true;
            return iterator;
        }

        public void close()
        {
            iterator.close();
        }
    }
}
