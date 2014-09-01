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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;

/**
 * Helper class to deserialize OnDiskAtom efficiently.
 *
 * More precisely, this class is used by the low-level readers
 * (IndexedSliceReader and SSTableNamesIterator) to ensure we don't
 * do more work than necessary (i.e. we don't allocate/deserialize
 * objects for things we don't care about).
 */
public abstract class AtomDeserializer
{
    protected final CFMetaData metadata;
    protected final DataInput in;
    protected final SerializationHelper helper;
    protected final Columns columns;

    protected AtomDeserializer(CFMetaData metadata,
                               DataInput in,
                               SerializationHelper helper,
                               Columns columns)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
        this.columns = columns;
    }

    public static AtomDeserializer create(CFMetaData metadata,
                                          DataInput in,
                                          SerializationHeader header,
                                          SerializationHelper helper,
                                          Columns columns)
    {
        if (helper.version >= MessagingService.VERSION_30)
            return new CurrentDeserializer(metadata, in, header, helper, columns);
        else
            throw new UnsupportedOperationException();
            //return new LegacyLayout.LegacyAtomDeserializer(metadata, in, flag, expireBefore, version, columns);
    }

    /**
     * Whether or not there is more atom to read.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Compare the provided bound to the next atom to read on disk.
     *
     * This will not read/deserialize the whole atom but only what is necessary for the
     * comparison. Whenever we know what to do with this atom (read it or skip it),
     * readNext or skipNext should be called.
     */
    public abstract int compareNextTo(Slice.Bound bound) throws IOException;

    /**
     * Returns whether the next atom is a row or not.
     */
    public abstract boolean nextIsRow() throws IOException;

    /**
     * Returns the next atom.
     */
    public abstract Atom readNext() throws IOException;

    /**
     * Clears any state in this deserializer.
     */
    public abstract void clearState() throws IOException;

    /**
     * Skips the next atom.
     */
    public abstract void skipNext() throws IOException;

    private static class CurrentDeserializer extends AtomDeserializer
    {
        private final ClusteringPrefix.Deserializer clusteringDeserializer;
        private final SerializationHeader header;

        private int nextFlags;
        private boolean isReady;
        private boolean isDone;

        private final ReusableRow row;
        private final ReusableRangeTombstoneMarker marker;

        private CurrentDeserializer(CFMetaData metadata,
                                    DataInput in,
                                    SerializationHeader header,
                                    SerializationHelper helper,
                                    Columns columns)
        {
            super(metadata, in, helper, columns);
            this.header = header;
            this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
            this.row = new ReusableRow(metadata.clusteringColumns().size(), header.columns().regulars, helper.nowInSec);
            this.marker = new ReusableRangeTombstoneMarker(metadata.clusteringColumns().size());
        }

        public boolean hasNext() throws IOException
        {
            if (isReady)
                return true;

            prepareNext();
            return !isDone;
        }

        private void prepareNext() throws IOException
        {
            if (isDone)
                return;

            nextFlags = in.readUnsignedByte();
            if (AtomSerializer.isEndOfPartition(nextFlags))
            {
                isDone = true;
                isReady = false;
                return;
            }

            clusteringDeserializer.prepare(nextFlags);
            isReady = true;
        }

        public int compareNextTo(Slice.Bound bound) throws IOException
        {
            if (!isReady)
                prepareNext();

            assert !isDone;

            return clusteringDeserializer.compareNextTo(bound);
        }

        public boolean nextIsRow() throws IOException
        {
            if (!isReady)
                prepareNext();

            return AtomSerializer.kind(nextFlags) == Atom.Kind.ROW;
        }

        public Atom readNext() throws IOException
        {
            isReady = false;
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                RangeTombstoneMarker.Writer writer = marker.writer();
                clusteringDeserializer.deserializeNextBound(writer);
                AtomSerializer.serializer.deserializeMarkerBody(in, header, helper, nextFlags, writer);
                return marker;
            }
            else
            {
                Row.Writer writer = row.writer();
                clusteringDeserializer.deserializeNextClustering(writer);
                AtomSerializer.serializer.deserializeRowBody(in, header, helper, columns, nextFlags, writer);
                return row;
            }
        }

        public void skipNext() throws IOException
        {
            isReady = false;
            clusteringDeserializer.skipNext();
            if (AtomSerializer.kind(nextFlags) == Atom.Kind.RANGE_TOMBSTONE_MARKER)
            {
                AtomSerializer.serializer.skipMarkerBody(in, header, nextFlags);
            }
            else
            {
                AtomSerializer.serializer.skipRowBody(in, header, helper, nextFlags);
            }
        }

        public void clearState()
        {
            isReady = false;
            isDone = false;
        }
    }
}
