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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnIndex
{
    public final List<IndexHelper.IndexInfo> columnsIndex;

    private static final ColumnIndex EMPTY = new ColumnIndex(Collections.<IndexHelper.IndexInfo>emptyList());

    private ColumnIndex(List<IndexHelper.IndexInfo> columnsIndex)
    {
        assert columnsIndex != null;

        this.columnsIndex = columnsIndex;
    }

    public static ColumnIndex writeAndBuildIndex(AtomIterator iterator, SequentialWriter output, SerializationHeader header, Version version) throws IOException
    {
        assert !AtomIterators.isEmpty(iterator) && version.storeRows();

        Builder builder = new Builder(iterator, output, header, version.correspondingMessagingVersion());
        return builder.build();
    }

    @VisibleForTesting
    public static ColumnIndex nothing()
    {
        return EMPTY;
    }

    /**
     * Help to create an index for a column family based on size of columns,
     * and write said columns to disk.
     */
    private static class Builder
    {
        private final AtomIterator iterator;
        private final SequentialWriter writer;
        private final SerializationHeader header;
        private final int version;

        private final ColumnIndex result;
        private final long initialPosition;
        private long startPosition = -1;

        private int atomWritten;

        private ClusteringPrefix firstClustering;
        private final ReusableClusteringPrefix lastClustering;

        private DeletionTime openMarker;

        public Builder(AtomIterator iterator,
                       SequentialWriter writer,
                       SerializationHeader header,
                       int version)
        {
            this.iterator = iterator;
            this.writer = writer;
            this.header = header;
            this.version = version;

            this.result = new ColumnIndex(new ArrayList<IndexHelper.IndexInfo>());
            this.initialPosition = writer.getFilePointer();
            this.lastClustering = new ReusableClusteringPrefix(iterator.metadata().clusteringColumns().size());
        }

        private void writePartitionHeader(AtomIterator iterator) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer.stream);
            DeletionTime.serializer.serialize(iterator.partitionLevelDeletion(), writer.stream);
            if (header.hasStatic())
                AtomSerializer.serializer.serialize(iterator.staticRow(), header, writer.stream, version);
        }

        public ColumnIndex build() throws IOException
        {
            writePartitionHeader(iterator);

            while (iterator.hasNext())
                add(iterator.next());

            return close();
        }

        private long currentPosition()
        {
            return writer.getFilePointer() - initialPosition;
        }

        private void addIndexBlock()
        {
            IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstClustering,
                                                                         lastClustering.get().takeAlias(),
                                                                         startPosition,
                                                                         currentPosition() - startPosition,
                                                                         openMarker);
            result.columnsIndex.add(cIndexInfo);
            firstClustering = null;
        }

        private void add(Atom atom) throws IOException
        {
            lastClustering.copy(atom.clustering());
            boolean isMarker = atom.kind() == Atom.Kind.RANGE_TOMBSTONE_MARKER;

            if (firstClustering == null)
            {
                // Beginning of an index block. Remember the start and position
                firstClustering = lastClustering.get().takeAlias();
                startPosition = currentPosition();

                // A read could start reading at the beginning of any index block, so if we have an
                // open range tombstone, we need to "repeat" it at the beginning of the block so a
                // reader that start by this block is aware of that ongoing deletion.
                // If we do have an an open marker, atom can only be either a Clustering, or a close marker.
                // If it's a close marker, then there is really nothing to do. If it's a clustering, we
                // close and re-open the current marker.
                if (openMarker != null && !isMarker)
                {
                    AtomSerializer.serializer.serialize(SimpleRangeTombstoneMarker.close(firstClustering, openMarker), header, writer.stream, version);
                    AtomSerializer.serializer.serialize(SimpleRangeTombstoneMarker.open(firstClustering, openMarker), header, writer.stream, version);
                    atomWritten += 2;
                }
            }

            AtomSerializer.serializer.serialize(atom, header, writer.stream, version);
            ++atomWritten;

            if (isMarker)
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker)atom;
                openMarker = marker.clustering().isStart() ? marker.deletionTime().takeAlias() : null;
            }

            // if we hit the column index size that we have to index after, go ahead and index it.
            if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                addIndexBlock();
        }

        private ColumnIndex close() throws IOException
        {
            AtomSerializer.serializer.writeEndOfPartition(writer.stream);

            // It's possible we add no atoms, just a top level deletion
            if (atomWritten == 0)
                return ColumnIndex.EMPTY;

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (firstClustering != null)
                addIndexBlock();

            // we should always have at least one computed index block, but we only write it out if there is more than that.
            assert result.columnsIndex.size() > 0;
            return result;
        }
    }
}
