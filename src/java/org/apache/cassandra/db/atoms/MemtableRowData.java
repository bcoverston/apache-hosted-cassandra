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

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Row data stored inside a memtable.
 *
 * This has methods like dataSize and unsharedHeapSizeExcludingData that are
 * specific to memtables.
 */
public interface MemtableRowData extends Clusterable
{
    public Columns columns();

    public int dataSize();

    // returns the size of the Row and all references on the heap, excluding any costs associated with byte arrays
    // that would be allocated by a clone operation, as these will be accounted for by the allocator
    public long unsharedHeapSizeExcludingData();

    public interface ReusableRow extends Row
    {
        public ReusableRow setTo(MemtableRowData rowData);
    }

    public class BufferRowData implements MemtableRowData
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferRowData(null, LivenessInfo.NONE, DeletionTime.LIVE, null));

        private final Clustering clustering;
        private final LivenessInfo livenessInfo;
        private final DeletionTime deletion;
        private final RowDataBlock dataBlock;

        public BufferRowData(Clustering clustering, LivenessInfo livenessInfo, DeletionTime deletion, RowDataBlock dataBlock)
        {
            this.clustering = clustering;
            this.livenessInfo = livenessInfo.takeAlias();
            this.deletion = deletion.takeAlias();
            this.dataBlock = dataBlock;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        public Columns columns()
        {
            return dataBlock.columns();
        }

        public int dataSize()
        {
            return clustering.dataSize() + livenessInfo.dataSize() + deletion.dataSize() + dataBlock.dataSize();
        }

        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE
                 + (clustering == Clustering.STATIC_CLUSTERING ? 0 : ((BufferClustering)clustering).unsharedHeapSizeExcludingData())
                 + dataBlock.unsharedHeapSizeExcludingData();
        }

        public static ReusableRow createReusableRow(int nowInSec)
        {
            return new BufferRow(nowInSec);
        }

        private static class BufferRow extends AbstractReusableRow implements ReusableRow
        {
            private final int nowInSec;
            private BufferRowData rowData;

            private BufferRow(int nowInSec)
            {
                this.nowInSec = nowInSec;
            }

            public ReusableRow setTo(MemtableRowData rowData)
            {
                assert rowData instanceof BufferRowData;
                this.rowData = (BufferRowData)rowData;
                return this;
            }

            protected RowDataBlock data()
            {
                return rowData.dataBlock;
            }

            protected int row()
            {
                return 0;
            }

            public Clustering clustering()
            {
                return rowData.clustering;
            }

            public LivenessInfo partitionKeyLivenessInfo()
            {
                return rowData.livenessInfo;
            }

            public boolean isLive(Cell cell)
            {
                return cell.isLive(nowInSec);
            }

            public long maxLiveTimestamp()
            {
                return Rows.computeMaxLiveTimestamp(this);
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public DeletionTime deletion()
            {
                return rowData.deletion;
            }
        }
    }

    public class BufferClustering extends Clustering
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferClustering(0));

        private final ByteBuffer[] values;

        public BufferClustering(int size)
        {
            this.values = new ByteBuffer[size];
        }

        public void setClusteringValue(int i, ByteBuffer value)
        {
            values[i] = value;
        }

        public int size()
        {
            return values.length;
        }

        public ByteBuffer get(int i)
        {
            return values[i];
        }

        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(values);
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
        }

        public Clustering takeAlias()
        {
            return this;
        }
    }

    public class BufferSliceBound extends Slice.Bound.SimpleBound
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferSliceBound(Kind.INCL_START_BOUND, new ByteBuffer[0]));

        private BufferSliceBound(Kind kind, ByteBuffer[] values)
        {
            super(kind, values);
        }

        public static BufferSliceBound clone(Slice.Bound bound, AbstractAllocator allocator)
        {
            ByteBuffer[] values = new ByteBuffer[bound.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = allocator.clone(bound.get(i));
            return new BufferSliceBound(bound.kind(), values);
        }

        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(values);
        }
    }

    public class BufferCellPath extends CellPath.SimpleCellPath
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCellPath(new ByteBuffer[0]));

        private BufferCellPath(ByteBuffer[] values)
        {
            super(values);
        }

        public static BufferCellPath clone(CellPath path, AbstractAllocator allocator)
        {
            int size = path.size();
            ByteBuffer[] values = new ByteBuffer[size];
            for (int i = 0; i < size; i++)
                values[i] = allocator.clone(path.get(0));
            return new BufferCellPath(values);
        }

        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(values);
        }
    }
}
