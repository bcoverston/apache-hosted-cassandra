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
package org.apache.cassandra.io.sstable.format.big;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StreamingHistogram;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    // not very random, but the only value that can't be mistaken for a legal column-name length
    public static final int END_OF_ROW = 0x0000;

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    public BigTableWriter(Descriptor descriptor, Long keyCount, Long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector, SerializationHeader header)
    {
        super(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector, header);

        if (compression)
        {
            dataFile = SequentialWriter.open(getFilename(),
                                             descriptor.filenameFor(Component.COMPRESSION_INFO),
                                             metadata.compressionParameters(),
                                             metadataCollector);
            dbuilder = SegmentedFile.getCompressedBuilder((CompressedSequentialWriter) dataFile);
        }
        else
        {
            dataFile = SequentialWriter.open(new File(getFilename()), new File(descriptor.filenameFor(Component.CRC)));
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        }
        iwriter = new IndexWriter(keyCount, dataFile);
    }

    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    private long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataEnd, RowIndexEntry index) throws IOException
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataEnd);
        iwriter.append(decoratedKey, index, dataEnd);
        dbuilder.addPotentialBoundary(dataEnd);
    }

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public RowIndexEntry append(AtomIterator iterator)
    {
        DecoratedKey key = iterator.partitionKey();

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return null;
        }

        if (AtomIterators.isEmpty(iterator))
            return null;

        long startPosition = beforeAppend(key);

        try (StatsCollector withStats = new StatsCollector(iterator, metadataCollector))
        {
            ColumnIndex index = ColumnIndex.writeAndBuildIndex(withStats, dataFile, header, descriptor.version);

            RowIndexEntry entry = RowIndexEntry.create(startPosition, iterator.partitionLevelDeletion(), index);

            long endPosition = dataFile.getFilePointer();
            metadataCollector.addPartitionSizeInBytes(endPosition - startPosition);
            afterAppend(key, endPosition, entry);
            return entry;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
    }

    private static class StatsCollector extends WrappingAtomIterator
    {
        private int cellCount;
        private final MetadataCollector collector;
        private final Set<ColumnDefinition> complexColumnsSetInRow = new HashSet<>();

        StatsCollector(AtomIterator iter, MetadataCollector collector)
        {
            super(iter);
            this.collector = collector;
            collector.update(iter.partitionLevelDeletion());
        }

        @Override
        public Atom next()
        {
            Atom atom = super.next();
            collector.updateClusteringValues(atom.clustering());

            switch (atom.kind())
            {
                case ROW:
                    Row row = (Row)atom;
                    collector.update(row.partitionKeyLivenessInfo());
                    collector.update(row.deletion());

                    int simpleColumnsSet = 0;
                    complexColumnsSetInRow.clear();

                    for (Cell cell : row)
                    {
                        if (cell.column().isComplex())
                            complexColumnsSetInRow.add(cell.column());
                        else
                            ++simpleColumnsSet;

                        ++cellCount;
                        collector.update(cell.livenessInfo());

                        if (cell.isCounterCell())
                            collector.updateHasLegacyCounterShards(CounterCells.hasLegacyShards(cell));
                    }

                    for (int i = 0; i < row.columns().complexColumnCount(); i++)
                        collector.update(row.getDeletion(row.columns().getComplex(i)));

                    collector.updateColumnSetPerRow(simpleColumnsSet + complexColumnsSetInRow.size());

                    break;
                case RANGE_TOMBSTONE_MARKER:
                    collector.update(((RangeTombstoneMarker)atom).deletionTime());
                    break;
            }
            return atom;
        }

        @Override
        public void close()
        {
            collector.addCellPerPartitionCount(cellCount);
            super.close();
        }
    }

    /**
     * After failure, attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void abort()
    {
        assert descriptor.type.isTemporary;
        if (iwriter == null && dataFile == null)
            return;

        if (iwriter != null)
            iwriter.abort();

        if (dataFile!= null)
            dataFile.abort();

        Set<Component> components = SSTable.componentsFor(descriptor);
        try
        {
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (FSWriteError e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
            throw e;
        }
    }

    // we use this method to ensure any managed data we may have retained references to during the write are no
    // longer referenced, so that we do not need to enclose the expensive call to closeAndOpenReader() in a transaction
    public void isolateReferences()
    {
        // currently we only maintain references to first/last/lastWrittenKey from the data provided; all other
        // data retention is done through copying
        first = getMinimalKey(first);
        last = lastWrittenKey = getMinimalKey(last);
    }

    private Descriptor makeTmpLinks()
    {
        // create temp links if they don't already exist
        Descriptor link = descriptor.asType(Descriptor.Type.TEMPLINK);
        if (!new File(link.filenameFor(Component.PRIMARY_INDEX)).exists())
        {
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), new File(link.filenameFor(Component.PRIMARY_INDEX)));
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.DATA)), new File(link.filenameFor(Component.DATA)));
        }
        return link;
    }

    public SSTableReader openEarly(long maxDataAge)
    {
        StatsMetadata sstableMetadata = (StatsMetadata) metadataCollector.finalizeMetadata(partitioner.getClass().getCanonicalName(),
                                                                                           metadata.getBloomFilterFpChance(),
                                                                                           repairedAt,
                                                                                           header).get(MetadataType.STATS);

        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = iwriter.getMaxReadable();
        if (boundary == null)
            return null;

        assert boundary.indexLength > 0 && boundary.dataLength > 0;
        Descriptor link = makeTmpLinks();
        // open the reader early, giving it a FINAL descriptor type so that it is indistinguishable for other consumers
        SegmentedFile ifile = iwriter.builder.complete(link.filenameFor(Component.PRIMARY_INDEX), boundary.indexLength);
        SegmentedFile dfile = dbuilder.complete(link.filenameFor(Component.DATA), boundary.dataLength);
        SSTableReader sstable = SSTableReader.internalOpen(descriptor.asType(Descriptor.Type.FINAL),
                                                           components, metadata,
                                                           partitioner, ifile,
                                                           dfile, iwriter.summary.build(partitioner, boundary),
                                                           iwriter.bf.sharedCopy(), maxDataAge, sstableMetadata, SSTableReader.OpenReason.EARLY, header);

        // now it's open, find the ACTUAL last readable key (i.e. for which the data file has also been flushed)
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(boundary.lastKey);
        return sstable;
    }

    public SSTableReader closeAndOpenReader()
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge)
    {
        return finish(FinishType.NORMAL, maxDataAge, this.repairedAt);
    }

    public SSTableReader finish(FinishType finishType, long maxDataAge, long repairedAt)
    {
        assert finishType != FinishType.CLOSE;
        Pair<Descriptor, StatsMetadata> p;

        p = close(finishType, repairedAt < 0 ? this.repairedAt : repairedAt);
        Descriptor desc = p.left;
        StatsMetadata metadata = p.right;

        if (finishType == FinishType.EARLY)
            desc = makeTmpLinks();

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(desc.filenameFor(Component.PRIMARY_INDEX), finishType.isFinal);
        SegmentedFile dfile = dbuilder.complete(desc.filenameFor(Component.DATA), finishType.isFinal);
        SSTableReader sstable = SSTableReader.internalOpen(desc.asType(Descriptor.Type.FINAL),
                                                           components,
                                                           this.metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary.build(partitioner),
                                                           iwriter.bf.sharedCopy(),
                                                           maxDataAge,
                                                           metadata,
                                                           finishType.openReason,
                                                           header);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);

        if (finishType.isFinal)
        {
            iwriter.bf.close();
            iwriter.summary.close();
            // try to save the summaries to disk
            sstable.saveSummary(iwriter.builder, dbuilder);
            iwriter = null;
            dbuilder = null;
        }
        return sstable;
    }

    // Close the writer and return the descriptor to the new sstable and it's metadata
    public Pair<Descriptor, StatsMetadata> close()
    {
        return close(FinishType.CLOSE, this.repairedAt);
    }

    private Pair<Descriptor, StatsMetadata> close(FinishType type, long repairedAt)
    {
        switch (type)
        {
            case EARLY: case CLOSE: case NORMAL:
            iwriter.close();
            dataFile.close();
            if (type == FinishType.CLOSE)
                iwriter.bf.close();
        }

        // write sstable statistics
        Map<MetadataType, MetadataComponent> metadataComponents;
        metadataComponents = metadataCollector
                             .finalizeMetadata(partitioner.getClass().getCanonicalName(),
                                               metadata.getBloomFilterFpChance(),
                                               repairedAt,
                                               header);

        // remove the 'tmp' marker from all components
        Descriptor descriptor = this.descriptor;
        if (type.isFinal)
        {
            dataFile.writeFullChecksum(descriptor);
            writeMetadata(descriptor, metadataComponents);
            // save the table of components
            SSTable.appendTOC(descriptor, components);
            descriptor = rename(descriptor, components);
        }

        return Pair.create(descriptor, (StatsMetadata) metadataComponents.get(MetadataType.STATS));
    }

    private static void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        SequentialWriter out = SequentialWriter.open(new File(desc.filenameFor(Component.STATS)));
        try
        {
            desc.getMetadataSerializer().serialize(components, out.stream);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
        finally
        {
            out.close();
        }
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter
    {
        private final SequentialWriter indexFile;
        public final SegmentedFile.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private FileMark mark;

        IndexWriter(long keyCount, final SequentialWriter dataFile)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummaryBuilder(keyCount, metadata.getMinIndexInterval(), Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata.getBloomFilterFpChance(), true);
            // register listeners to be alerted when the data files are flushed
            indexFile.setPostFlushListener(new Runnable()
            {
                public void run()
                {
                    summary.markIndexSynced(indexFile.getLastFlushOffset());
                }
            });
            dataFile.setPostFlushListener(new Runnable()
            {
                public void run()
                {
                    summary.markDataSynced(dataFile.getLastFlushOffset());
                }
            });
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry, long dataEnd) throws IOException
        {
            bf.add(key);
            long indexStart = indexFile.getFilePointer();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile.stream);
                rowIndexEntrySerializer.serialize(indexEntry, indexFile.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }
            long indexEnd = indexFile.getFilePointer();

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);

            summary.maybeAddEntry(key, indexStart, indexEnd, dataEnd);
            builder.addPotentialBoundary(indexStart);
        }

        public void abort()
        {
            summary.close();
            indexFile.abort();
            bf.close();
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try
                {
                    // bloom filter
                    FileOutputStream fos = new FileOutputStream(path);
                    DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos);
                    FilterFactory.serialize(bf, stream);
                    stream.flush();
                    fos.getFD().sync();
                    stream.close();
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }

            // index
            long position = indexFile.getFilePointer();
            indexFile.close(); // calls force
            FileUtils.truncate(indexFile.getPath(), position);
        }

        public void mark()
        {
            mark = indexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
        }
    }
}
