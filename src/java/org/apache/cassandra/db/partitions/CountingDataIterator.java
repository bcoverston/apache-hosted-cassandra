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

import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.DataLimits;

public class CountingDataIterator extends WrappingDataIterator
{
    protected final DataLimits.RowCounter counter;

    public CountingDataIterator(DataIterator result, DataLimits.RowCounter counter)
    {
        super(result);
        this.counter = counter;
    }

    public CountingDataIterator(DataIterator result, DataLimits limits)
    {
        this(result, limits.newRowCounter(true));
    }

    public DataLimits.RowCounter counter()
    {
        return counter;
    }

    @Override
    public boolean hasNext()
    {
        if (counter.isDone())
            return false;

        return super.hasNext();
    }

    @Override
    public RowIterator next()
    {
        RowIterator iter = super.next();
        counter.newPartition(iter.partitionKey());
        return new CountingRowIterator(iter, counter);
    }
}
