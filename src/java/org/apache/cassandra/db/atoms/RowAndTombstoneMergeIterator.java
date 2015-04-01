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

import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.db.*;

public class RowAndTombstoneMergeIterator extends UnmodifiableIterator<Atom> implements PeekingIterator<Atom>
{
    private final Comparator<Clusterable> comparator;

    private Iterator<Row> rowIter;
    private Row nextRow;

    private Iterator<RangeTombstone> tombstoneIter;
    private RangeTombstone nextTombstone;
    private boolean inTombstone;
    private final ReusableRangeTombstoneMarker marker;

    private Atom next;

    public RowAndTombstoneMergeIterator(ClusteringComparator comparator, boolean reversed)
    {
        this.comparator = reversed ? comparator.reversed() : comparator;
        this.marker = new ReusableRangeTombstoneMarker(comparator.size());
    }

    public RowAndTombstoneMergeIterator setTo(Iterator<Row> rowIter, Iterator<RangeTombstone> tombstoneIter)
    {
        this.rowIter = rowIter;
        this.tombstoneIter = tombstoneIter;
        this.nextRow = null;
        this.nextTombstone = null;
        this.next = null;
        this.inTombstone = false;
        return this;
    }

    public boolean isSet()
    {
        return rowIter != null;
    }

    private void prepareNext()
    {
        if (next != null)
            return;

        if (nextTombstone == null && tombstoneIter.hasNext())
            nextTombstone = tombstoneIter.next();
        if (nextRow == null && rowIter.hasNext())
            nextRow = rowIter.next();

        if (nextTombstone == null)
        {
            if (nextRow == null)
                return;

            next = nextRow;
            nextRow = null;
        }
        else if (nextRow == null)
        {
            if (inTombstone)
            {
                RangeTombstone rt = nextTombstone;
                nextTombstone = null;
                next = marker.setTo(rt.deletedSlice().end(), rt.deletionTime());
            }
            else
            {
                inTombstone = true;
                next = marker.setTo(nextTombstone.deletedSlice().start(), nextTombstone.deletionTime());
            }
        }
        else if (inTombstone)
        {
            if (comparator.compare(nextTombstone.deletedSlice().end(), nextRow.clustering()) < 0)
            {
                RangeTombstone rt = nextTombstone;
                nextTombstone = null;
                next = marker.setTo(rt.deletedSlice().end(), rt.deletionTime());
            }
            else
            {
                next = nextRow;
                nextRow = null;
            }
        }
        else
        {
            if (comparator.compare(nextTombstone.deletedSlice().start(), nextRow.clustering()) < 0)
            {
                inTombstone = true;
                next = marker.setTo(nextTombstone.deletedSlice().start(), nextTombstone.deletionTime());
            }
            else
            {
                next = nextRow;
                nextRow = null;
            }
        }
    }

    public boolean hasNext()
    {
        prepareNext();
        return next != null;
    }

    public Atom next()
    {
        prepareNext();
        Atom toReturn = next;
        next = null;
        return toReturn;
    }

    public Atom peek()
    {
        prepareNext();
        return next();
    }
}
