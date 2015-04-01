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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * Abstract class to create AtomIterator that lazily initialize themselves.
 *
 * This is used during partition range queries when we know the partition key but want
 * to defer the initialization of the rest of the AtomIterator until we need those informations.
 * See {@link BigTableScanner#KeyScanningIterator} for instance.
 */
public abstract class LazilyInitializedAtomIterator extends AbstractIterator<Atom> implements AtomIterator
{
    private final DecoratedKey partitionKey;

    private AtomIterator iterator;

    public LazilyInitializedAtomIterator(DecoratedKey partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    protected abstract AtomIterator initializeIterator();

    private void maybeInit()
    {
        if (iterator == null)
            iterator = initializeIterator();
    }

    public CFMetaData metadata()
    {
        maybeInit();
        return iterator.metadata();
    }

    public PartitionColumns columns()
    {
        maybeInit();
        return iterator.columns();
    }

    public boolean isReverseOrder()
    {
        maybeInit();
        return iterator.isReverseOrder();
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        maybeInit();
        return iterator.partitionLevelDeletion();
    }

    public Row staticRow()
    {
        maybeInit();
        return iterator.staticRow();
    }

    public AtomStats stats()
    {
        maybeInit();
        return iterator.stats();
    }

    public int nowInSec()
    {
        maybeInit();
        return iterator.nowInSec();
    }

    protected Atom computeNext()
    {
        maybeInit();
        return iterator.hasNext() ? iterator.next() : endOfData();
    }

    public void close()
    {
        if (iterator != null)
            iterator.close();
    }
}
