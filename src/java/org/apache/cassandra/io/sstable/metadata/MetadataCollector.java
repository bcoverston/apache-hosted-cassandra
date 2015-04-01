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
package org.apache.cassandra.io.sstable.metadata;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.StreamingHistogram;

public class MetadataCollector
{
    public static final double NO_COMPRESSION_RATIO = -1.0;

    static EstimatedHistogram defaultCellPerPartitionCountHistogram()
    {
        // EH of 114 can track a max value of 2395318855, i.e., > 2B columns
        return new EstimatedHistogram(114);
    }

    static EstimatedHistogram defaultPartitionSizeHistogram()
    {
        // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
        return new EstimatedHistogram(150);
    }

    static StreamingHistogram defaultTombstoneDropTimeHistogram()
    {
        return new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
    }

    public static StatsMetadata defaultStatsMetadata()
    {
        return new StatsMetadata(defaultPartitionSizeHistogram(),
                                 defaultCellPerPartitionCountHistogram(),
                                 ReplayPosition.NONE,
                                 Long.MIN_VALUE,
                                 Long.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 0,
                                 Integer.MAX_VALUE,
                                 NO_COMPRESSION_RATIO,
                                 defaultTombstoneDropTimeHistogram(),
                                 0,
                                 Collections.<ByteBuffer>emptyList(),
                                 Collections.<ByteBuffer>emptyList(),
                                 true,
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 -1,
                                 -1);
    }

    protected EstimatedHistogram estimatedPartitionSize = defaultPartitionSizeHistogram();
    // TODO: cound the number of row per partition (either with the number of cells, or instead)
    protected EstimatedHistogram estimatedCellPerPartitionCount = defaultCellPerPartitionCountHistogram();
    protected ReplayPosition replayPosition = ReplayPosition.NONE;
    protected final MinMaxLongTracker timestampTracker = new MinMaxLongTracker();
    protected final MinMaxIntTracker localDeletionTimeTracker = new MinMaxIntTracker(LivenessInfo.NO_DELETION_TIME, LivenessInfo.NO_DELETION_TIME);
    protected final MinMaxIntTracker ttlTracker = new MinMaxIntTracker(LivenessInfo.NO_TTL, LivenessInfo.NO_TTL);
    protected double compressionRatio = NO_COMPRESSION_RATIO;
    protected Set<Integer> ancestors = new HashSet<>();
    protected StreamingHistogram estimatedTombstoneDropTime = defaultTombstoneDropTimeHistogram();
    protected int sstableLevel;
    protected ByteBuffer[] minClusteringValues;
    protected ByteBuffer[] maxClusteringValues;
    protected boolean hasLegacyCounterShards = false;
    protected long totalColumnsSet;
    protected long totalRows;

    /**
     * Default cardinality estimation method is to use HyperLogLog++.
     * Parameter here(p=13, sp=25) should give reasonable estimation
     * while lowering bytes required to hold information.
     * See CASSANDRA-5906 for detail.
     */
    protected ICardinality cardinality = new HyperLogLogPlus(13, 25);
    private final ClusteringComparator comparator;

    public MetadataCollector(ClusteringComparator comparator)
    {
        this.comparator = comparator;

        this.minClusteringValues = new ByteBuffer[comparator.size()];
        this.maxClusteringValues = new ByteBuffer[comparator.size()];
    }

    public MetadataCollector(Collection<SSTableReader> sstables, ClusteringComparator comparator, int level)
    {
        this(comparator);

        replayPosition(ReplayPosition.getReplayPosition(sstables));
        sstableLevel(level);
        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        for (SSTableReader sstable : sstables)
        {
            addAncestor(sstable.descriptor.generation);
            for (Integer i : sstable.getAncestors())
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    addAncestor(i);
        }
    }

    public MetadataCollector addKey(ByteBuffer key)
    {
        long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
        cardinality.offerHashed(hashed);
        return this;
    }

    public MetadataCollector addPartitionSizeInBytes(long partitionSize)
    {
        estimatedPartitionSize.add(partitionSize);
        return this;
    }

    public MetadataCollector addCellPerPartitionCount(long cellCount)
    {
        estimatedCellPerPartitionCount.add(cellCount);
        return this;
    }

    public MetadataCollector mergeTombstoneHistogram(StreamingHistogram histogram)
    {
        estimatedTombstoneDropTime.merge(histogram);
        return this;
    }

    /**
     * Ratio is compressed/uncompressed and it is
     * if you have 1.x then compression isn't helping
     */
    public MetadataCollector addCompressionRatio(long compressed, long uncompressed)
    {
        compressionRatio = (double) compressed/uncompressed;
        return this;
    }

    public MetadataCollector update(LivenessInfo newInfo)
    {
        if (newInfo.hasTimestamp())
            updateTimestamp(newInfo.timestamp());
        if (newInfo.hasTTL())
            updateTTL(newInfo.ttl());
        if (newInfo.hasLocalDeletionTime())
            updateLocalDeletionTime(newInfo.localDeletionTime());
        return this;
    }

    public MetadataCollector update(DeletionTime dt)
    {
        if (!dt.isLive())
        {
            updateTimestamp(dt.markedForDeleteAt());
            updateLocalDeletionTime(dt.localDeletionTime());
        }
        return this;
    }

    public MetadataCollector updateColumnSetPerRow(long columnSetInRow)
    {
        totalColumnsSet += columnSetInRow;
        ++totalRows;
        return this;
    }

    private void updateTimestamp(long newTimestamp)
    {
        timestampTracker.update(newTimestamp);
    }

    private void updateLocalDeletionTime(int newLocalDeletionTime)
    {
        localDeletionTimeTracker.update(newLocalDeletionTime);
        estimatedTombstoneDropTime.update(newLocalDeletionTime);
    }

    private void updateTTL(int newTTL)
    {
        ttlTracker.update(newTTL);
    }

    public MetadataCollector replayPosition(ReplayPosition replayPosition)
    {
        this.replayPosition = replayPosition;
        return this;
    }

    public MetadataCollector addAncestor(int generation)
    {
        this.ancestors.add(generation);
        return this;
    }

    public MetadataCollector sstableLevel(int sstableLevel)
    {
        this.sstableLevel = sstableLevel;
        return this;
    }

    public MetadataCollector updateClusteringValues(ClusteringPrefix clustering)
    {
        int size = clustering.size();
        for (int i = 0; i < size; i++)
        {
            AbstractType<?> type = comparator.subtype(i);
            ByteBuffer newValue = clustering.get(i);
            minClusteringValues[i] = min(minClusteringValues[i], newValue, type);
            maxClusteringValues[i] = max(maxClusteringValues[i], newValue, type);
        }
        return this;
    }

    private static ByteBuffer min(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator)
    {
        if (b1 == null)
            return b2;
        if (b2 == null)
            return b1;

        if (comparator.compare(b1, b2) >= 0)
            return b2;
        return b1;
    }

    private static ByteBuffer max(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator)
    {
        if (b1 == null)
            return b2;
        if (b2 == null)
            return b1;

        if (comparator.compare(b1, b2) >= 0)
            return b1;
        return b2;
    }

    public MetadataCollector updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
    {
        this.hasLegacyCounterShards = this.hasLegacyCounterShards || hasLegacyCounterShards;
        return this;
    }

    public Map<MetadataType, MetadataComponent> finalizeMetadata(String partitioner, double bloomFilterFPChance, long repairedAt, SerializationHeader header)
    {
        Map<MetadataType, MetadataComponent> components = Maps.newHashMap();
        components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
        components.put(MetadataType.STATS, new StatsMetadata(estimatedPartitionSize,
                                                             estimatedCellPerPartitionCount,
                                                             replayPosition,
                                                             timestampTracker.min(),
                                                             timestampTracker.max(),
                                                             localDeletionTimeTracker.min(),
                                                             localDeletionTimeTracker.max(),
                                                             ttlTracker.min(),
                                                             ttlTracker.max(),
                                                             compressionRatio,
                                                             estimatedTombstoneDropTime,
                                                             sstableLevel,
                                                             makeList(minClusteringValues),
                                                             makeList(maxClusteringValues),
                                                             hasLegacyCounterShards,
                                                             repairedAt,
                                                             totalColumnsSet,
                                                             totalRows));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(ancestors, cardinality));
        components.put(MetadataType.HEADER, header.toComponent());
        return components;
    }

    private static List<ByteBuffer> makeList(ByteBuffer[] values)
    {
        // In most case, l will be the same size than values, but it's possible for it to be smaller
        List<ByteBuffer> l = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++)
            if (values[i] == null)
                break;
            else
                l.add(values[i]);
        return l;
    }

    public static class MinMaxLongTracker
    {
        private final long defaultMin;
        private final long defaultMax;

        private boolean isSet = false;
        private long min;
        private long max;

        public MinMaxLongTracker()
        {
            this(Long.MIN_VALUE, Long.MAX_VALUE);
        }

        public MinMaxLongTracker(long defaultMin, long defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(long value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public long min()
        {
            return isSet ? min : defaultMin;
        }

        public long max()
        {
            return isSet ? max : defaultMax;
        }
    }

    public static class MinMaxIntTracker
    {
        private final int defaultMin;
        private final int defaultMax;

        private boolean isSet = false;
        private int min;
        private int max;

        public MinMaxIntTracker()
        {
            this(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        public MinMaxIntTracker(int defaultMin, int defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(int value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public int min()
        {
            return isSet ? min : defaultMin;
        }

        public int max()
        {
            return isSet ? max : defaultMax;
        }
    }
}
