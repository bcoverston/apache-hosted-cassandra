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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.security.MessageDigest;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static methods to work with row iterators.
 */
public abstract class RowIterators
{
    private static final Logger logger = LoggerFactory.getLogger(RowIterators.class);

    private RowIterators() {}

    /**
     * Returns whether the provided iterator has no data (including no deletion data).
     */
    public static boolean isEmpty(RowIterator iterator)
    {
        return !iterator.hasNext() && iterator.staticRow().isEmpty();
    }

    public static PartitionUpdate toUpdate(RowIterator iterator)
    {
        PartitionUpdate update = new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), iterator.columns(), 1, iterator.nowInSec());

        if (iterator.staticRow() != Rows.EMPTY_STATIC_ROW)
            iterator.staticRow().copyTo(update.staticWriter());

        while (iterator.hasNext())
            iterator.next().copyTo(update.writer());

        return update;
    }

    public static void digest(RowIterator iterator, MessageDigest digest)
    {
        // TODO: we're not computing digest the same way that old nodes. This
        // means we'll have digest mismatches during upgrade. Is this ok? Computing
        // digest as before will be a tad complex (you'd have to reconstruct the
        // cell names etc...)
        digest.update(iterator.partitionKey().getKey().duplicate());
        iterator.columns().digest(digest);
        FBUtilities.updateWithBoolean(digest, iterator.isReverseOrder());
        iterator.staticRow().digest(digest);

        while (iterator.hasNext())
            iterator.next().digest(digest);
    }

    public static RowIterator emptyIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final boolean isReverseOrder, final int nowInSec)
    {
        return new RowIterator()
        {
            public CFMetaData metadata()
            {
                return cfm;
            }

            public boolean isReverseOrder()
            {
                return isReverseOrder;
            }

            public PartitionColumns columns()
            {
                return PartitionColumns.NONE;
            }

            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public boolean hasNext()
            {
                return false;
            }

            public Row next()
            {
                throw new NoSuchElementException();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
            }
        };
    }

    /**
     * Wraps the provided iterator so it logs the returned rows for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static RowIterator loggingIterator(RowIterator iterator, final String id)
    {
        CFMetaData metadata = iterator.metadata();
        logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}",
                    new Object[]{ id,
                                  metadata.ksName,
                                  metadata.cfName,
                                  metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                                  iterator.isReverseOrder() });

        return new WrappingRowIterator(iterator)
        {
            @Override
            public Row next()
            {
                Row next = super.next();
                logger.info("[{}] {}", id, next.toString(metadata()));
                return next;
            }
        };
    }
}
