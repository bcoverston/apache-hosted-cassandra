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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

abstract class AbstractSSTableIterator implements SliceableAtomIterator
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractSSTableIterator.class);

    protected final SSTableReader sstable;
    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final PartitionColumns columns;
    protected final SerializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      DecoratedKey key,
                                      RowIndexEntry indexEntry,
                                      PartitionColumns columns,
                                      int nowInSec)
    {
        this.sstable = sstable;
        this.key = key;
        this.columns = columns;
        this.helper = new SerializationHelper(sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL, nowInSec);

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            try
            {
                boolean shouldCloseFile = file == null;
                boolean isAtPartitionStart = false;
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     and we need to read the partition deletion time.
                //   - we're querying static columns.
                if (indexEntry.isIndexed() && columns.statics.isEmpty())
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                }
                else
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (file == null)
                        file = sstable.getFileDataInput(indexEntry.position);
                    else
                        file.seek(indexEntry.position);

                    ByteBufferUtil.skipShortLength(file); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
                    if (sstable.header.hasStatic())
                    {
                        if (columns.statics.isEmpty())
                        {
                            this.staticRow = Rows.EMPTY_STATIC_ROW;
                            AtomSerializer.serializer.skipStaticRow(file, sstable.header, helper);
                        }
                        else
                        {
                            this.staticRow = AtomSerializer.serializer.deserializeStaticRow(file, sstable.header, helper, columns.statics);
                        }
                    }
                    else
                    {
                        this.staticRow = Rows.EMPTY_STATIC_ROW;
                    }
                    isAtPartitionStart = true;
                }

                this.reader = createReader(indexEntry, file, isAtPartitionStart, shouldCloseFile);
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, file.getPath());
            }
        }
    }

    protected abstract Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean isAtPartitionStart, boolean shouldCloseFile);

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public AtomStats stats()
    {
        // We could return sstable.header.stats(), but this may not be as accurate than the actual sstable stats (see
        // CompactionTask.makeSerializationHeader for details) so we use the latter instead.
        return new AtomStats(sstable.getMinTimestamp(), sstable.getMinLocalDeletionTime(), sstable.getMinTTL(), sstable.getAvgColumnSetPerRow());
    }

    public int nowInSec()
    {
        return helper.nowInSec;
    }

    public boolean hasNext()
    {
        try
        {
            return reader != null && reader.hasNext();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public Atom next()
    {
        try
        {
            assert reader != null;
            return reader.next();
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public Iterator<Atom> slice(Slice slice)
    {
        try
        {
            if (reader == null)
                return Collections.emptyIterator();

            return reader.slice(slice);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        if (reader != null)
        {
            try
            {
                reader.close();
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }
    }

    protected abstract class Reader
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;

        protected AtomDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        protected DeletionTime openMarker = null;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;
            if (file != null)
                createDeserializer();
        }

        private void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = AtomDeserializer.create(sstable.metadata, file, sstable.header, helper, columns.regulars);
        }

        protected void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            if (file == null)
            {
                file = sstable.getFileDataInput(position);
                createDeserializer();
            }
            else
            {
                file.seek(position);
                deserializer.clearState();
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            openMarker = marker.clustering().isStart() ? marker.deletionTime().takeAlias() : null;
        }

        protected DeletionTime getAndClearOpenMarker()
        {
            DeletionTime toReturn = openMarker;
            openMarker = null;
            return toReturn;
        }

        public abstract boolean hasNext() throws IOException;
        public abstract Atom next() throws IOException;
        public abstract Iterator<Atom> slice(Slice slice) throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    protected abstract class IndexedReader extends Reader
    {
        protected final RowIndexEntry indexEntry;
        protected final List<IndexHelper.IndexInfo> indexes;

        protected int currentIndexIdx = -1;

        // Marks the beginning of the block corresponding to currentIndexIdx.
        protected FileMark mark;

        // !isInit means we have never seeked in the file and thus shouldn't read as we could be anywhere
        protected boolean isInit;

        protected IndexedReader(FileDataInput file, boolean shouldCloseFile, RowIndexEntry indexEntry, boolean isInit)
        {
            super(file, shouldCloseFile);
            this.indexEntry = indexEntry;
            this.indexes = indexEntry.columnsIndex();
            this.isInit = isInit;
        }

        // Should be called when we're at the beginning of blockIdx.
        protected void updateBlock(int blockIdx) throws IOException
        {
            seekToPosition(indexEntry.position + indexes.get(blockIdx).offset);

            currentIndexIdx = blockIdx;
            openMarker = blockIdx > 0 ? indexes.get(blockIdx - 1).endOpenMarker : null;
            mark = file.mark();
        }

        public IndexHelper.IndexInfo currentIndex()
        {
            return indexes.get(currentIndexIdx);
        }

        public IndexHelper.IndexInfo previousIndex()
        {
            return currentIndexIdx <= 1 ? null : indexes.get(currentIndexIdx - 1);
        }
    }
}
