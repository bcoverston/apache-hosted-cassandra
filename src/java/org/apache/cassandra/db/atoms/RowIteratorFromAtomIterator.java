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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

public class RowIteratorFromAtomIterator extends AbstractIterator<Row> implements RowIterator
{
    private static final Logger logger = LoggerFactory.getLogger(RowIteratorFromAtomIterator.class);

    private final AtomIterator iter;
    private final TombstoneFilteringRow filter;

    public RowIteratorFromAtomIterator(AtomIterator iter)
    {
        this.iter = iter;
        this.filter = new TombstoneFilteringRow();
    }

    public CFMetaData metadata()
    {
        return iter.metadata();
    }

    public boolean isReverseOrder()
    {
        return iter.isReverseOrder();
    }

    public PartitionColumns columns()
    {
        return iter.columns();
    }

    public DecoratedKey partitionKey()
    {
        return iter.partitionKey();
    }

    public Row staticRow()
    {
        Row row = iter.staticRow();
        return row.isEmpty() ? row : new TombstoneFilteringRow().setTo(row);
    }

    public int nowInSec()
    {
        return iter.nowInSec();
    }

    protected Row computeNext()
    {
        while (iter.hasNext())
        {
            Atom next = iter.next();
            if (next.kind() != Atom.Kind.ROW)
                continue;

            Row row = filter.setTo((Row)next);
            if (!row.isEmpty())
                return row;
        }
        return endOfData();
    }

    public void close()
    {
        iter.close();
    }
}
