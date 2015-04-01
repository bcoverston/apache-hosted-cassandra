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

import java.io.IOException;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Ordering;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.AbstractBounds.Boundary;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

public class BigTableScanner implements ISSTableScanner
{
    protected final RandomAccessReader dfile;
    protected final RandomAccessReader ifile;
    public final SSTableReader sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private final DataRange dataRange;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final int nowInSec;

    protected PartitionIterator iterator;

    // Full scan of the sstables
    public static ISSTableScanner getScanner(SSTableReader sstable, RateLimiter limiter, int nowInSec)
    {
        return new BigTableScanner(sstable, null, limiter, nowInSec, Iterators.singletonIterator(fullRange(sstable)));
    }

    public static ISSTableScanner getScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter, int nowInSec)
    {
        return new BigTableScanner(sstable, dataRange, limiter, nowInSec, makeBounds(sstable, dataRange).iterator());
    }

    public static ISSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter, int nowInSec)
    {
        // We want to avoid allocating a SSTableScanner if the range don't overlap the sstable (#5249)
        List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(tokenRanges);
        if (positions.isEmpty())
            return new EmptySSTableScanner(sstable.getFilename());

        return new BigTableScanner(sstable, null, limiter, nowInSec, makeBounds(sstable, tokenRanges).iterator());
    }

    private BigTableScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter, int nowInSec, Iterator<AbstractBounds<RowPosition>> rangeIterator)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = dataRange;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata,
                                                                                                        sstable.descriptor.version,
                                                                                                        sstable.header);
        this.nowInSec = nowInSec;
        this.rangeIterator = rangeIterator;
    }

    private static List<AbstractBounds<RowPosition>> makeBounds(SSTableReader sstable, Collection<Range<Token>> tokenRanges)
    {
        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(tokenRanges.size());
        for (Range<Token> range : Range.normalize(tokenRanges))
            addRange(sstable, Range.makeRowRange(range), boundsList);
        return boundsList;
    }

    private static List<AbstractBounds<RowPosition>> makeBounds(SSTableReader sstable, DataRange dataRange)
    {
        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        addRange(sstable, dataRange.keyRange(), boundsList);
        return boundsList;
    }

    private static AbstractBounds<RowPosition> fullRange(SSTableReader sstable)
    {
        return new Bounds<RowPosition>(sstable.first, sstable.last);
    }

    private static void addRange(SSTableReader sstable, AbstractBounds<RowPosition> requested, List<AbstractBounds<RowPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range)requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                Boundary<RowPosition> left = new Boundary<RowPosition>(sstable.first, true);
                Boundary<RowPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                Boundary<RowPosition> right = new Boundary<RowPosition>(sstable.last, true);
                Boundary<RowPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.first, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert requested.left.compareTo(requested.right) <= 0 || requested.right.isMinimum();
            Boundary<RowPosition> left, right;
            left = requested.leftBoundary();
            right = requested.rightBoundary();
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new Boundary<RowPosition>(sstable.last, true)
                                                    : minRight(right, sstable.last, true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
    }

    private void seekToCurrentRangeStart()
    {
        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        ifile.seek(indexPosition);
        try
        {

            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.Serializer.skip(ifile);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close()
    {
        try
        {
            FileUtils.close(dfile, ifile);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public AtomIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private PartitionIterator createIterator()
    {
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<AtomIterator> implements PartitionIterator
    {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;

        protected AtomIterator computeNext()
        {
            try
            {
                if (nextEntry == null)
                {
                    do
                    {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                            return endOfData();

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = rowIndexEntrySerializer.deserialize(ifile);
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                if (ifile.isEOF())
                {
                    nextEntry = null;
                    nextKey = null;
                }
                else
                {
                    // we need the position of the start of the next key, regardless of whether it falls in the current range
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = rowIndexEntrySerializer.deserialize(ifile);

                    if (!currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                /*
                 * For a given partition key, we want to avoid hitting the data
                 * file unless we're explicitely asked to. This is important
                 * for PartitionRangeReadCommand#checkCacheFilter.
                 */
                return new LazilyInitializedAtomIterator(currentKey)
                {
                    protected AtomIterator initializeIterator()
                    {
                        try
                        {
                            if (dataRange == null)
                            {
                                dfile.seek(currentEntry.position + currentEntry.headerOffset());
                                ByteBufferUtil.readWithShortLength(dfile); // key
                                return new SSTableIdentityIterator(sstable, dfile, partitionKey(), nowInSec);
                            }

                            PartitionFilter filter = dataRange.partitionFilter(partitionKey());
                            return filter.filter(sstable.iterator(dfile, partitionKey(), currentEntry, filter.queriedColumns(), filter.isReversed(), nowInSec));
                        }
                        catch (IOException e)
                        {
                            sstable.markSuspect();
                            throw new CorruptSSTableException(e, sstable.getFilename());
                        }
                    }
                };
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }

        public void close()
        {
            BigTableScanner.this.close();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "dfile=" + dfile +
               " ifile=" + ifile +
               " sstable=" + sstable +
               ")";
    }

    public static class EmptySSTableScanner extends AbstractPartitionIterator implements ISSTableScanner
    {
        private final String filename;

        public EmptySSTableScanner(String filename)
        {
            this.filename = filename;
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public String getBackingFiles()
        {
            return filename;
        }

        public boolean hasNext()
        {
            return false;
        }

        public AtomIterator next()
        {
            return null;
        }
    }
}
