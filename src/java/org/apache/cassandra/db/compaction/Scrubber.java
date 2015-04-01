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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

public class Scrubber implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final File destination;
    private final boolean skipCorrupted;

    private final CompactionController controller;
    private final boolean isCommutative;
    private final boolean isIndex;
    private final boolean checkData;
    private final int expectedBloomFilterSize;

    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final ScrubInfo scrubInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private final boolean isOffline;

    private SSTableReader newSstable;
    private SSTableReader newInOrderSstable;

    private int goodRows;
    private int badRows;
    private int emptyRows;

    private final OutputHandler outputHandler;

    private static final Comparator<Partition> partitionComparator = new Comparator<Partition>()
    {
         public int compare(Partition r1, Partition r2)
         {
             return r1.partitionKey().compareTo(r2.partitionKey());
         }
    };
    private final SortedSet<Partition> outOfOrder = new TreeSet<>(partitionComparator);

    public Scrubber(ColumnFamilyStore cfs, SSTableReader sstable, boolean skipCorrupted, boolean isOffline) throws IOException
    {
        this(cfs, sstable, skipCorrupted, new OutputHandler.LogOutput(), isOffline);
    }

    public Scrubber(ColumnFamilyStore cfs, SSTableReader sstable, boolean skipCorrupted, OutputHandler outputHandler, boolean isOffline) throws IOException
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;
        this.skipCorrupted = skipCorrupted;
        this.isOffline = isOffline;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata,
                                                                                                        sstable.descriptor.version,
                                                                                                        sstable.header);

        List<SSTableReader> toScrub = Collections.singletonList(sstable);

        // Calculate the expected compacted filesize
        this.destination = cfs.directories.getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(toScrub, OperationType.SCRUB));
        if (destination == null)
            throw new IOException("disk full");

        // If we run scrub offline, we should never purge tombstone, as we cannot know if other sstable have data that the tombstone deletes.
        this.controller = isOffline
                        ? new ScrubController(cfs)
                        : new CompactionController(cfs, Collections.singleton(sstable), CompactionManager.getDefaultGcBefore(cfs));
        this.isCommutative = cfs.metadata.isCounter();
        this.isIndex = cfs.isIndex();
        this.checkData = !this.isIndex; //LocalByPartitionerType does not support validation
        this.expectedBloomFilterSize = Math.max(cfs.metadata.getMinIndexInterval(), (int)(SSTableReader.getApproximateKeyCount(toScrub)));

        // loop through each row, deserializing to check for damage.
        // we'll also loop through the index at the same time, using the position from the index to recover if the
        // row header (key or data size) is corrupt. (This means our position in the index file will be one row
        // "ahead" of the data file.)
        this.dataFile = isOffline
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)));
        this.scrubInfo = new ScrubInfo(dataFile, sstable);
    }

    private AtomIterator withValidation(AtomIterator iter, String filename)
    {
        return checkData ? AtomIterators.withValidation(iter, filename) : iter;
    }

    public void scrub()
    {
        outputHandler.output(String.format("Scrubbing %s (%s bytes)", sstable, dataFile.length()));
        Set<SSTableReader> oldSSTable = Sets.newHashSet(sstable);
        SSTableRewriter writer = new SSTableRewriter(cfs, oldSSTable, sstable.maxDataAge, isOffline);
        int nowInSec = FBUtilities.nowInSeconds();
        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                // throw away variable so we don't have a side effect in the assert
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserialize(indexFile).position;
                assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
            }

            writer.switchWriter(CompactionManager.createWriter(cfs, destination, expectedBloomFilterSize, sstable.getSSTableMetadata().repairedAt, sstable));

            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {
                if (scrubInfo.isStopRequested())
                    throw new CompactionInterruptedException(scrubInfo.getCompactionInfo());
                long rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                             ? dataFile.length()
                                             : rowIndexEntrySerializer.deserialize(indexFile).position;
                }
                catch (Throwable th)
                {
                    JVMStabilityInspector.inspectThrowable(th);
                    outputHandler.warn("Error reading index file", th);
                    nextIndexKey = null;
                    nextRowPositionFromIndex = dataFile.length();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining();
                long dataSizeFromIndex = nextRowPositionFromIndex - dataStartFromIndex;

                long dataSize = dataSizeFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s bytes", keyName, dataSize));

                assert currentIndexKey != null || indexFile.isEOF();

                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read row key from data file"));
                    if (dataSize > dataFile.length())
                        throw new IOError(new IOException("Impossible row size " + dataSize));

                    try (AtomIterator atoms = withValidation(new SSTableIdentityIterator(sstable, dataFile, key, nowInSec), dataFile.getPath()))
                    {
                        if (prevKey != null && prevKey.compareTo(key) > 0)
                        {
                            saveOutOfOrderRow(prevKey, key, atoms);
                            continue;
                        }

                        if (writer.tryAppend(atoms) == null)
                            emptyRows++;
                        else
                            goodRows++;
                    }

                    prevKey = key;
                    if (!key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex)
                        outputHandler.warn("Index file contained a different key or row size; using key from data file");
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    outputHandler.warn("Error reading row (stacktrace follows):", th);

                    if (currentIndexKey != null
                        && (key == null || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex || dataSize != dataSizeFromIndex))
                    {
                        outputHandler.output(String.format("Retrying from row index; data is %s bytes starting at %s",
                                                  dataSizeFromIndex, dataStartFromIndex));
                        key = sstable.partitioner.decorateKey(currentIndexKey);
                        try
                        {
                            AtomIterator atoms = withValidation(new SSTableIdentityIterator(sstable, dataFile, key, nowInSec), dataFile.getPath());
                            if (prevKey != null && prevKey.compareTo(key) > 0)
                            {
                                saveOutOfOrderRow(prevKey, key, atoms);
                                continue;
                            }

                            if (writer.tryAppend(atoms) == null)
                                emptyRows++;
                            else
                                goodRows++;
                            prevKey = key;
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            throwIfCannotContinue(key, th2);

                            outputHandler.warn("Retry failed too. Skipping to next row (retry's stacktrace follows)", th2);
                            dataFile.seek(nextRowPositionFromIndex);
                            badRows++;
                        }
                    }
                    else
                    {
                        throwIfCannotContinue(key, th);

                        outputHandler.warn("Row starting at position " + dataStart + " is unreadable; skipping to next");
                        if (currentIndexKey != null)
                            dataFile.seek(nextRowPositionFromIndex);
                        badRows++;
                    }
                }
            }

            if (!outOfOrder.isEmpty())
            {
                // out of order rows, but no bad rows found - we can keep our repairedAt time
                long repairedAt = badRows > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt;
                SSTableWriter inOrderWriter = CompactionManager.createWriter(cfs, destination, expectedBloomFilterSize, repairedAt, sstable);
                for (Partition partition : outOfOrder)
                    inOrderWriter.append(partition.atomIterator(partition.columns(), Slices.ALL, false, nowInSec));
                newInOrderSstable = inOrderWriter.closeAndOpenReader(sstable.maxDataAge);
                if (!isOffline)
                    cfs.getDataTracker().addSSTables(Collections.singleton(newInOrderSstable));
                outputHandler.warn(String.format("%d out of order rows found while scrubbing %s; Those have been written (in order) to a new sstable (%s)", outOfOrder.size(), sstable, newInOrderSstable));
            }

            // finish obsoletes the old sstable
            List<SSTableReader> finished = writer.finish(badRows > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt);
            if (!finished.isEmpty())
                newSstable = finished.get(0);
            if (!isOffline)
                cfs.getDataTracker().markCompactedSSTablesReplaced(oldSSTable, finished, OperationType.SCRUB);
        }
        catch (Throwable t)
        {
            writer.abort();
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();
        }

        if (newSstable == null)
        {
            if (badRows > 0)
                outputHandler.warn("No valid rows found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
            else
                outputHandler.output("Scrub of " + sstable + " complete; looks like all " + emptyRows + " rows were tombstoned");
        }
        else
        {
            outputHandler.output("Scrub of " + sstable + " complete: " + goodRows + " rows in new sstable and " + emptyRows + " empty (tombstoned) rows dropped");
            if (badRows > 0)
                outputHandler.warn("Unable to recover " + badRows + " rows that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
        }
    }

    private void saveOutOfOrderRow(DecoratedKey prevKey, DecoratedKey key, AtomIterator atoms)
    {
        // TODO bitch if the row is too large?  if it is there's not much we can do ...
        outputHandler.warn(String.format("Out of order row detected (%s found after %s)", key, prevKey));
        outOfOrder.add(ArrayBackedPartition.create(atoms));
    }

    public SSTableReader getNewSSTable()
    {
        return newSstable;
    }

    public SSTableReader getNewInOrderSSTable()
    {
        return newInOrderSstable;
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void throwIfCannotContinue(DecoratedKey key, Throwable th)
    {
        if (isIndex)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s' for an index table. " +
                                             "Scrubbing will abort for this table and the index will be rebuilt.", key));
            throw new IOError(th);
        }

        if (isCommutative && !skipCorrupted)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the row with key '%s'.  Skipping corrupt " +
                                             "rows in counter tables will result in undercounts for the affected " +
                                             "counters (see CASSANDRA-2759 for more details), so by default the scrub will " +
                                             "stop at this point.  If you would like to skip the row anyway and continue " +
                                             "scrubbing, re-run the scrub with the --skip-corrupted option.", key));
            throw new IOError(th);
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexFile);
    }

    public CompactionInfo.Holder getScrubInfo()
    {
        return scrubInfo;
    }

    private static class ScrubInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;

        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.SCRUB,
                                          dataFile.getFilePointer(),
                                          dataFile.length());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    private static class ScrubController extends CompactionController
    {
        public ScrubController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public long maxPurgeableTimestamp(DecoratedKey key)
        {
            return Long.MIN_VALUE;
        }
    }
}
