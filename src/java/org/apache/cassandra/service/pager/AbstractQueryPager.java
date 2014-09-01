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
package org.apache.cassandra.service.pager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    private final ConsistencyLevel consistencyLevel;
    private final boolean localQuery;

    protected final CFMetaData cfm;
    protected final DataLimits limits;

    private int remaining;
    private int remainingInPartition = -1;
    private boolean exhausted;

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 boolean localQuery,
                                 CFMetaData cfm,
                                 DataLimits limits)
    {
        this.consistencyLevel = consistencyLevel;
        this.localQuery = localQuery;

        this.cfm = cfm;
        this.limits = limits;

        this.remaining = limits.count();
    }

    public DataIterator fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return DataIterators.EMPTY;

        pageSize = Math.min(pageSize, remaining);
        return new PagerIterator(queryNextPage(pageSize, consistencyLevel, localQuery), limits.forPaging(pageSize));
    }

    private class PagerIterator extends CountingDataIterator
    {
        private final DataLimits limits;

        private DecoratedKey lastKey;
        private Row lastRow;

        private PagerIterator(DataIterator iter, DataLimits limits)
        {
            super(iter, limits);
            this.limits = limits;
        }

        @Override
        public RowIterator next()
        {
            RowIterator iter = super.next();
            lastKey = iter.partitionKey();
            remainingInPartition = limits.perPartitionCount();
            return new RowPagerIterator(iter);
        }

        @Override
        public void close()
        {
            super.close();
            recordLast(lastKey, lastRow);

            int counted = counter.counted();
            remaining -= counted;
            remainingInPartition -= counter.countedInCurrentPartition();
            exhausted = counted < limits.count();
        }

        private class RowPagerIterator extends WrappingRowIterator
        {
            RowPagerIterator(RowIterator iter)
            {
                super(iter);
            }

            @Override
            public Row next()
            {
                lastRow = super.next();
                return lastRow;
            }
        }
    }

    protected void restoreState(int remaining, int remainingInPartition)
    {
        this.remaining = remaining;
        this.remainingInPartition = remainingInPartition;
    }

    public boolean isExhausted()
    {
        return exhausted || remaining == 0 || ((this instanceof SinglePartitionPager) && remainingInPartition == 0);
    }

    public int maxRemaining()
    {
        return remaining;
    }

    // Only meaningful after a call to fetchPage or after having called restoreState, will return -1 otherwise.
    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    protected abstract DataIterator queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery) throws RequestValidationException, RequestExecutionException;

    protected abstract void recordLast(DecoratedKey key, Row row);
}
