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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Utility class to handle deserializing atom from sstables.
 *
 * Note that this is not a full fledged AtomIterator. It's also not closeable, it is always
 * the job of the user to close the underlying ressources.
 */
public abstract class SSTableAtomIterator extends AbstractIterator<Atom> implements Iterator<Atom>
{
    private final DataInput in;
    private final SerializationHeader header;
    private final SerializationHelper helper;

    private final ReusableRow row;
    private final ReusableRangeTombstoneMarker marker;

    public SSTableAtomIterator(DataInput in, SerializationHeader header, SerializationHelper helper)
    {
        this.in = in;
        this.helper = helper;
        this.header = header;

        this.row = new ReusableRow(header.clusteringTypes().size(), header.columns().regulars, helper.nowInSec);
        this.marker = new ReusableRangeTombstoneMarker(header.clusteringTypes().size());
    }

    protected Row readStaticRow() throws IOException
    {
        return header.hasStatic()
             ? AtomSerializer.serializer.deserializeStaticRow(in, header, helper)
             : Rows.EMPTY_STATIC_ROW;
    }

    protected Atom computeNext()
    {
        try
        {
            Atom.Kind kind = AtomSerializer.serializer.deserialize(in, header, helper, row.writer(), marker.writer());

            return kind == null
                 ? endOfData()
                 : (kind == Atom.Kind.ROW ? updateRow(row) : marker);
        }
        catch (IOException e)
        {
            throw onIOException(e);
        }
    }

    // Used by StreamReader to mark local shard to be cleared for counter.
    protected Row updateRow(ReusableRow row)
    {
        return row;
    }

    public int nowInSec()
    {
        return helper.nowInSec;
    }

    protected abstract RuntimeException onIOException(IOException exception);
}
