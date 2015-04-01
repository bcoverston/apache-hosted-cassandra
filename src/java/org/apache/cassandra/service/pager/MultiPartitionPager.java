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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;

/**
 * Pager over a list of ReadCommand.
 *
 * Note that this is not easy to make efficient. Indeed, we need to page the first command fully before
 * returning results from the next one, but if the result returned by each command is small (compared to pageSize),
 * paging the commands one at a time under-performs compared to parallelizing. On the other, if we parallelize
 * and each command raised pageSize results, we'll end up with commands.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 *
 * For now, we keep it simple (somewhat) and just do one command at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though if we later want to get fancy, we could use the
 * cfs meanRowSize to decide if parallelizing some of the command might be worth it while being confident we don't
 * blow out memory.
 */
class MultiPartitionPager implements QueryPager
{
    private final SinglePartitionPager[] pagers;
    private final DataLimits limit;

    private int remaining;
    private int current;

    MultiPartitionPager(List<SinglePartitionReadCommand<?>> commands, ConsistencyLevel consistencyLevel, ClientState cState, boolean localQuery, PagingState state, DataLimits limit)
    {
        this.limit = limit;

        int i = 0;
        // If it's not the beginning (state != null), we need to find where we were and skip previous commands
        // since they are done.
        if (state != null)
            for (; i < commands.size(); i++)
                if (commands.get(i).partitionKey().getKey().equals(state.partitionKey))
                    break;

        if (i >= commands.size())
        {
            pagers = null;
            return;
        }

        pagers = new SinglePartitionPager[commands.size() - i];
        // 'i' is on the first non exhausted pager for the previous page (or the first one)
        pagers[0] = QueryPagers.pager(commands.get(i), consistencyLevel, cState, localQuery, state);
        int nowInSec = commands.get(i).nowInSec();

        // Following ones haven't been started yet
        for (int j = i + 1; j < commands.size(); j++)
        {
            SinglePartitionReadCommand command = commands.get(j);
            if (command.nowInSec() != nowInSec)
                throw new IllegalArgumentException("All commands must have the same timestamp or weird results may happen.");
            pagers[j - i] = QueryPagers.pager(command, consistencyLevel, cState, localQuery, null);
        }

        remaining = state == null ? limit.count() : state.remaining;
    }

    public PagingState state()
    {
        // Sets current to the first non-exhausted pager
        if (isExhausted())
            return null;

        PagingState state = pagers[current].state();
        return new PagingState(pagers[current].key(), state == null ? null : state.cellName, remaining, Integer.MAX_VALUE);
    }

    public boolean isExhausted()
    {
        if (remaining <= 0 || pagers == null)
            return true;

        while (current < pagers.length)
        {
            if (!pagers[current].isExhausted())
                return false;

            current++;
        }
        return true;
    }

    public DataIterator fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        int toQuery = Math.min(remaining, pageSize);
        PagersIterator iter = new PagersIterator(toQuery);
        CountingDataIterator countingIter = new CountingDataIterator(iter, limit.forPaging(toQuery));
        iter.setCounter(countingIter.counter());
        return countingIter;
    }

    private class PagersIterator extends AbstractIterator<RowIterator> implements DataIterator
    {
        private final int pageSize;
        private DataIterator result;
        private DataLimits.RowCounter counter;

        public PagersIterator(int pageSize)
        {
            this.pageSize = pageSize;
        }

        public void setCounter(DataLimits.RowCounter counter)
        {
            this.counter = counter;
        }

        protected RowIterator computeNext()
        {
            while (result == null || !result.hasNext())
            {
                // This sets us on the first non-exhausted pager
                if (isExhausted())
                    return endOfData();

                if (result != null)
                    result.close();

                result = pagers[current].fetchPage(pageSize - counter.counted());
            }
            return result.next();
        }

        public void close()
        {
            remaining -= counter.counted();
            if (result != null)
                result.close();
        }
    }

    public int maxRemaining()
    {
        return remaining;
    }
}
