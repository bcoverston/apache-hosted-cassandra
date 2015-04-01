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

import org.apache.cassandra.db.*;

// TODO rename to FilteringAtomIterator for consistency
public class RowFilteringAtomIterator extends WrappingAtomIterator
{
    private final FilteringRow filter;
    private Atom next;

    public RowFilteringAtomIterator(AtomIterator toFilter)
    {
        this(toFilter, null);
    }

    public RowFilteringAtomIterator(AtomIterator toFilter, FilteringRow filter)
    {
        super(toFilter);
        this.filter = filter;
    }

    protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        return true;
    }

    protected boolean includeRow(Row row)
    {
        return true;
    }

    protected boolean includePartitionDeletion(DeletionTime dt)
    {
        return true;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        DeletionTime dt = wrapped.partitionLevelDeletion();
        return includePartitionDeletion(dt) ? dt : DeletionTime.LIVE;
    }

    @Override
    public Row staticRow()
    {
        Row row = super.staticRow();
        if (row == Rows.EMPTY_STATIC_ROW)
            return row;

        if (filter != null)
            row = filter.setTo(row);

        return !row.isEmpty() && includeRow(row) ? row : Rows.EMPTY_STATIC_ROW;
    }

    @Override
    public boolean hasNext()
    {
        if (next != null)
            return true;

        while (super.hasNext())
        {
            Atom atom = super.next();
            if (atom.kind() == Atom.Kind.ROW)
            {
                Row row = filter == null ? (Row)atom : filter.setTo((Row)atom);
                if (!row.isEmpty() && includeRow(row))
                {
                    // We set next to the unfiltered atom on purpose, and well re-set the filter to it in next(). This avoids
                    // that if staticRow() is called while hasNext() has been called, we end up sending the (wrong) static row
                    next = atom;
                    return true;
                }
            }
            else
            {
                if (includeRangeTombstoneMarker((RangeTombstoneMarker)atom))
                {
                    next = atom;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Atom next()
    {
        if (next == null)
            hasNext();

        Atom toReturn = next;
        next = null;
        return toReturn.kind() == Atom.Kind.ROW && filter != null ? filter.setTo((Row)toReturn) : toReturn;
    }
}
