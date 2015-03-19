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

import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * Abstract class to make writing atom iterators that wrap another iterator
 * easier. By default, the wrapping iterator simply delegate every call to
 * the wrapped iterator so concrete implementations will override some of the
 * methods.
 */
public abstract class WrappingAtomIterator extends UnmodifiableIterator<Atom>  implements AtomIterator
{
    protected final AtomIterator wrapped;

    protected WrappingAtomIterator(AtomIterator wrapped)
    {
        this.wrapped = wrapped;
    }

    public CFMetaData metadata()
    {
        return wrapped.metadata();
    }

    public PartitionColumns columns()
    {
        return wrapped.columns();
    }

    public boolean isReverseOrder()
    {
        return wrapped.isReverseOrder();
    }

    public DecoratedKey partitionKey()
    {
        return wrapped.partitionKey();
    }

    public DeletionTime partitionLevelDeletion()
    {
        return wrapped.partitionLevelDeletion();
    }

    public int nowInSec()
    {
        return wrapped.nowInSec();
    }

    public Row staticRow()
    {
        return wrapped.staticRow();
    }

    public boolean hasNext()
    {
        return wrapped.hasNext();
    }

    public Atom next()
    {
        return wrapped.next();
    }

    public AtomStats stats()
    {
        return wrapped.stats();
    }

    public void close()
    {
        wrapped.close();
    }
}