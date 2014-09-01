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

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

public abstract class WrappingRow extends AbstractRow
{
    protected Row wrapped;

    private final ReusableIterator cellIterator = new ReusableIterator();

    /**
     * Apply some filtering/transformation on cells. This function
     * can return {@code null} in which case the cell will be ignored.
     */
    protected abstract Cell filterCell(Cell cell);

    public WrappingRow setTo(Row row)
    {
        this.wrapped = row;
        return this;
    }

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    public Clustering clustering()
    {
        return wrapped.clustering();
    }

    public Columns columns()
    {
        return wrapped.columns();
    }

    public LivenessInfo partitionKeyLivenessInfo()
    {
        return wrapped.partitionKeyLivenessInfo();
    }

    public long maxLiveTimestamp()
    {
        return wrapped.maxLiveTimestamp();
    }

    public int nowInSec()
    {
        return wrapped.nowInSec();
    }

    public DeletionTime deletion()
    {
        return wrapped.deletion();
    }

    public boolean hasComplexDeletion()
    {
        // Note that because cells can be filtered out/transformed through
        // filterCell(), we can't rely on wrapped.hasComplexDeletion().
        for (int i = 0; i < columns().complexColumnCount(); i++)
            if (!getDeletion(columns().getComplex(i)).isLive())
                return true;
        return false;
    }

    public Cell getCell(ColumnDefinition c)
    {
        Cell cell = wrapped.getCell(c);
        return cell == null ? null : filterCell(cell);
    }

    public Cell getCell(ColumnDefinition c, CellPath path)
    {
        Cell cell = wrapped.getCell(c, path);
        return cell == null ? null : filterCell(cell);
    }

    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        Iterator<Cell> cells = wrapped.getCells(c);
        if (cells == null)
            return null;

        cellIterator.setTo(cells);
        return cellIterator.hasNext() ? cellIterator : null;
    }

    public DeletionTime getDeletion(ColumnDefinition c)
    {
        return wrapped.getDeletion(c);
    }

    public Iterator<Cell> iterator()
    {
        return cellIterator.setTo(wrapped.iterator());
    }

    public Row takeAlias()
    {
        if (isStatic())
        {
            StaticRow.Builder builder = StaticRow.builder(columns(), nowInSec());
            copyTo(builder);
            return builder.build();
        }
        else
        {
            ReusableRow copy = new ReusableRow(clustering().size(), columns(), nowInSec());
            copyTo(copy.writer());
            return copy;
        }
    }

    private class ReusableIterator extends UnmodifiableIterator<Cell>
    {
        private Iterator<Cell> iter;
        private Cell next;

        public ReusableIterator setTo(Iterator<Cell> iter)
        {
            this.iter = iter;
            this.next = null;
            return this;
        }

        public boolean hasNext()
        {
            while (next == null && iter.hasNext())
                next = filterCell(iter.next());
            return next != null;
        }

        public Cell next()
        {
            if (next == null && !hasNext())
                throw new NoSuchElementException();

            Cell result = next;
            next = null;
            return result;
        }
    };
}
