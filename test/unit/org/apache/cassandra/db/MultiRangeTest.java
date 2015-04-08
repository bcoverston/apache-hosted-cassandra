/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import com.google.common.collect.Iterators;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.PartitionRangeReadBuilder;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class MultiRangeTest
{
    static byte[] bytes1, bytes2;
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String CF_STANDARD1 = "Standard1";

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
    }

    // TODO: Do we need to port these tests for ReadCommands?

//    @SuppressWarnings("unchecked")
//    @Test
//    public void testMultiRangeSomeEmptyNoIndex() throws Throwable
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] ranges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colA")),
//                new ColumnSlice(cellname("colC"), cellname("colE")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colI"), Composites.EMPTY) };
//
//        ColumnSlice[] rangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colI")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colE"), cellname("colC")),
//                new ColumnSlice(cellname("colA"), Composites.EMPTY) };
//
//        String tableName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace table = Keyspace.open(tableName);
//        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "i" };
//        Cell[] cols = new Cell[letters.length];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
//                    ByteBuffer.wrap(new byte[1]), 1);
//        }
//
//        putColsStandard(cfs, dk("a"), cols);
//
//        cfs.forceBlockingFlush();
//
//        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
//        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
//        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
//        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);
//
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colI");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colD", "colC", "colA");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colD", "colC");
//    }
//    */
//
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testMultiRangeSomeEmptyIndexed() throws Throwable
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] ranges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colA")),
//                new ColumnSlice(cellname("colC"), cellname("colE")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colI"), Composites.EMPTY) };
//
//        ColumnSlice[] rangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY,  cellname("colI")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colE"), cellname("colC")),
//                new ColumnSlice(cellname("colA"), Composites.EMPTY) };
//
//        String tableName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace table = Keyspace.open(tableName);
//        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "i" };
//        Cell[] cols = new Cell[letters.length];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
//                    ByteBuffer.wrap(new byte[1366]), 1);
//        }
//
//        putColsStandard(cfs, dk("a"), cols);
//
//        cfs.forceBlockingFlush();
//
//        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
//        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
//        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
//        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);
//
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colI");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colD", "colC", "colA");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colD", "colC");
//    }
//
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testMultiRangeContiguousNoIndex() throws Throwable
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] ranges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colA")),
//                new ColumnSlice(cellname("colC"), cellname("colE")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colI"), Composites.EMPTY) };
//
//        ColumnSlice[] rangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colI")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colE"), cellname("colC")),
//                new ColumnSlice(cellname("colA"), Composites.EMPTY) };
//
//        String tableName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace table = Keyspace.open(tableName);
//        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
//        Cell[] cols = new Cell[letters.length];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
//                    ByteBuffer.wrap(new byte[1]), 1);
//        }
//
//        putColsStandard(cfs, dk("a"), cols);
//
//        cfs.forceBlockingFlush();
//
//        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
//        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
//        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
//        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);
//
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colF", "colG", "colI");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colF", "colE", "colD", "colC", "colA");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colF");
//
//    }
//
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testMultiRangeContiguousIndexed() throws Throwable
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] ranges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colA")),
//                new ColumnSlice(cellname("colC"), cellname("colE")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colI"), Composites.EMPTY) };
//
//        ColumnSlice[] rangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colI")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colF"), cellname("colF")),
//                new ColumnSlice(cellname("colE"), cellname("colC")),
//                new ColumnSlice(cellname("colA"), Composites.EMPTY) };
//
//        String tableName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace table = Keyspace.open(tableName);
//        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
//        Cell[] cols = new Cell[letters.length];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
//                    ByteBuffer.wrap(new byte[1366]), 1);
//        }
//
//        putColsStandard(cfs, dk("a"), cols);
//
//        cfs.forceBlockingFlush();
//
//        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
//        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
//        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
//        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);
//
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colF", "colG", "colI");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colF", "colE", "colD", "colC", "colA");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colF");
//
//    }
//
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testMultiRangeIndexed() throws Throwable
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] ranges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colA")),
//                new ColumnSlice(cellname("colC"), cellname("colE")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colI"), Composites.EMPTY) };
//
//        ColumnSlice[] rangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colI")),
//                new ColumnSlice(cellname("colG"), cellname("colG")),
//                new ColumnSlice(cellname("colE"), cellname("colC")),
//                new ColumnSlice(cellname("colA"), Composites.EMPTY) };
//
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
//        Cell[] cols = new Cell[letters.length];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i].toUpperCase()),
//                    // use 1366 so that three cols make an index segment
//                    ByteBuffer.wrap(new byte[1366]), 1);
//        }
//
//        putColsStandard(cfs, dk("a"), cols);
//
//        cfs.forceBlockingFlush();
//
//        // this setup should generate the following row (assuming indexes are of 4Kb each):
//        // [colA, colB, colC, colD, colE, colF, colG, colH, colI]
//        // indexed as:
//        // index0 [colA, colC]
//        // index1 [colD, colF]
//        // index2 [colG, colI]
//        // and we're looking for the ranges:
//        // range0 [____, colA]
//        // range1 [colC, colE]
//        // range2 [colG, ColG]
//        // range3 [colI, ____]
//
//        SliceQueryFilter multiRangeForward = new SliceQueryFilter(ranges, false, 100);
//        SliceQueryFilter multiRangeForwardWithCounting = new SliceQueryFilter(ranges, false, 3);
//        SliceQueryFilter multiRangeReverse = new SliceQueryFilter(rangesReversed, true, 100);
//        SliceQueryFilter multiRangeReverseWithCounting = new SliceQueryFilter(rangesReversed, true, 3);
//
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForward, "a", "colA", "colC", "colD", "colE", "colG", "colI");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeForwardWithCounting, "a", "colA", "colC", "colD");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverse, "a", "colI", "colG", "colE", "colD", "colC", "colA");
//        findRowGetSlicesAndAssertColsFound(cfs, multiRangeReverseWithCounting, "a", "colI", "colG", "colE");
//
//    }
//
//    @Test
//    public void testMultipleRangesSlicesNoIndexedColumns() throws Throwable
//    {
//        // small values so that cols won't be indexed
//        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(10, true));
//    }
//
//    @Test
//    public void testMultipleRangesSlicesWithIndexedColumns() throws Throwable
//    {
//        // min val size before cols are indexed is 4kb while testing so lets make sure cols are indexed
//        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(1024, true));
//    }
//
//    @Test
//    public void testMultipleRangesSlicesInMemory() throws Throwable
//    {
//        // small values so that cols won't be indexed
//        testMultiRangeSlicesBehavior(prepareMultiRangeSlicesTest(10, false));
//    }
//
//    private ColumnFamilyStore prepareMultiRangeSlicesTest(int valueSize, boolean flush) throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        String[] letters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" };
//        Cell[] cols = new Cell[12];
//        for (int i = 0; i < cols.length; i++)
//        {
//            cols[i] = new BufferCell(cellname("col" + letters[i]), ByteBuffer.wrap(new byte[valueSize]), 1);
//        }
//
//        for (int i = 0; i < 12; i++)
//        {
//            putColsStandard(cfs, dk(letters[i]), Arrays.copyOfRange(cols, 0, i + 1));
//        }
//
//        if (flush)
//        {
//            cfs.forceBlockingFlush();
//        }
//        else
//        {
//            // The intent is to validate memtable code, so check we really didn't flush
//            assert cfs.getSSTables().isEmpty();
//        }
//
//        return cfs;
//    }
//
//    private void testMultiRangeSlicesBehavior(ColumnFamilyStore cfs)
//    {
//        // in order not to change thrift interfaces at this stage we build SliceQueryFilter
//        // directly instead of using QueryFilter to build it for us
//        ColumnSlice[] startMiddleAndEndRanges = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colc")),
//                new ColumnSlice(cellname("colf"), cellname("colg")),
//                new ColumnSlice(cellname("colj"), Composites.EMPTY) };
//
//        ColumnSlice[] startMiddleAndEndRangesReversed = new ColumnSlice[] {
//                new ColumnSlice(Composites.EMPTY, cellname("colj")),
//                new ColumnSlice(cellname("colg"), cellname("colf")),
//                new ColumnSlice(cellname("colc"), Composites.EMPTY) };
//
//        ColumnSlice[] startOnlyRange =
//                new ColumnSlice[] { new ColumnSlice(Composites.EMPTY, cellname("colc")) };
//
//        ColumnSlice[] startOnlyRangeReversed =
//                new ColumnSlice[] { new ColumnSlice(cellname("colc"), Composites.EMPTY) };
//
//        ColumnSlice[] middleOnlyRanges =
//                new ColumnSlice[] { new ColumnSlice(cellname("colf"), cellname("colg")) };
//
//        ColumnSlice[] middleOnlyRangesReversed =
//                new ColumnSlice[] { new ColumnSlice(cellname("colg"), cellname("colf")) };
//
//        ColumnSlice[] endOnlyRanges =
//                new ColumnSlice[] { new ColumnSlice(cellname("colj"), Composites.EMPTY) };
//
//        ColumnSlice[] endOnlyRangesReversed =
//                new ColumnSlice[] { new ColumnSlice(Composites.EMPTY, cellname("colj")) };
//
//        SliceQueryFilter startOnlyFilter = new SliceQueryFilter(startOnlyRange, false,
//                Integer.MAX_VALUE);
//        SliceQueryFilter startOnlyFilterReversed = new SliceQueryFilter(startOnlyRangeReversed, true,
//                Integer.MAX_VALUE);
//        SliceQueryFilter startOnlyFilterWithCounting = new SliceQueryFilter(startOnlyRange, false, 1);
//        SliceQueryFilter startOnlyFilterReversedWithCounting = new SliceQueryFilter(startOnlyRangeReversed,
//                true, 1);
//
//        SliceQueryFilter middleOnlyFilter = new SliceQueryFilter(middleOnlyRanges,
//                false,
//                Integer.MAX_VALUE);
//        SliceQueryFilter middleOnlyFilterReversed = new SliceQueryFilter(middleOnlyRangesReversed, true,
//                Integer.MAX_VALUE);
//        SliceQueryFilter middleOnlyFilterWithCounting = new SliceQueryFilter(middleOnlyRanges, false, 1);
//        SliceQueryFilter middleOnlyFilterReversedWithCounting = new SliceQueryFilter(middleOnlyRangesReversed,
//                true, 1);
//
//        SliceQueryFilter endOnlyFilter = new SliceQueryFilter(endOnlyRanges, false,
//                Integer.MAX_VALUE);
//        SliceQueryFilter endOnlyReversed = new SliceQueryFilter(endOnlyRangesReversed, true,
//                Integer.MAX_VALUE);
//        SliceQueryFilter endOnlyWithCounting = new SliceQueryFilter(endOnlyRanges, false, 1);
//        SliceQueryFilter endOnlyWithReversedCounting = new SliceQueryFilter(endOnlyRangesReversed,
//                true, 1);
//
//        SliceQueryFilter startMiddleAndEndFilter = new SliceQueryFilter(startMiddleAndEndRanges, false,
//                Integer.MAX_VALUE);
//        SliceQueryFilter startMiddleAndEndFilterReversed = new SliceQueryFilter(startMiddleAndEndRangesReversed, true,
//                Integer.MAX_VALUE);
//        SliceQueryFilter startMiddleAndEndFilterWithCounting = new SliceQueryFilter(startMiddleAndEndRanges, false,
//                1);
//        SliceQueryFilter startMiddleAndEndFilterReversedWithCounting = new SliceQueryFilter(
//                startMiddleAndEndRangesReversed, true,
//                1);
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "a", "cola");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "a", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "a", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "a", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "a", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "a", "cola");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "c", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "c", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "c", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "c", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "c", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "c", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "c", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "c", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "c", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "c", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "c", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "f", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "f", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "f", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "f", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "f", "colf");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "f", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "f", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "f", "colf");
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "f", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "f", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "f", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "f", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "f", "cola", "colb", "colc", "colf");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "f", "colf", "colc", "colb",
//                "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "f", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "f", "colf");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "h", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "h", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "h", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "h", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "h", "colf", "colg");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "h", "colg", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "h", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "h", "colg");
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "h", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "h", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "h", new String[] {});
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "h", new String[] {});
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "h", "cola", "colb", "colc", "colf",
//                "colg");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "h", "colg", "colf", "colc", "colb",
//                "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "h", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "h", "colg");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "j", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "j", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "j", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "j", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "j", "colf", "colg");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "j", "colg", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "j", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "j", "colg");
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "j", "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "j", "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "j", "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "j", "colj");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "j", "cola", "colb", "colc", "colf", "colg",
//                "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "j", "colj", "colg", "colf", "colc",
//                "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "j", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "j", "colj");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilter, "l", "cola", "colb", "colc");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversed, "l", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterWithCounting, "l", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startOnlyFilterReversedWithCounting, "l", "colc");
//
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilter, "l", "colf", "colg");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversed, "l", "colg", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterWithCounting, "l", "colf");
//        findRowGetSlicesAndAssertColsFound(cfs, middleOnlyFilterReversedWithCounting, "l", "colg");
//
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyFilter, "l", "colj", "colk", "coll");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyReversed, "l", "coll", "colk", "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithCounting, "l", "colj");
//        findRowGetSlicesAndAssertColsFound(cfs, endOnlyWithReversedCounting, "l", "coll");
//
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilter, "l", "cola", "colb", "colc", "colf", "colg",
//                "colj", "colk", "coll");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversed, "l", "coll", "colk", "colj", "colg",
//                "colf", "colc", "colb", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterWithCounting, "l", "cola");
//        findRowGetSlicesAndAssertColsFound(cfs, startMiddleAndEndFilterReversedWithCounting, "l", "coll");
//    }
//
//    private void findRowGetSlicesAndAssertColsFound(ColumnFamilyStore cfs, SliceQueryFilter filter, String rowKey,
//            String... colNames)
//    {
//        List<Row> rows = cfs.getRangeSlice(new Bounds<RowPosition>(rp(rowKey), rp(rowKey)),
//                                           null,
//                                           filter,
//                                           Integer.MAX_VALUE,
//                                           System.currentTimeMillis(),
//                                           false,
//                                           false);
//        assertSame("unexpected number of rows ", 1, rows.size());
//        Row row = rows.get(0);
//        Collection<Cell> cols = !filter.isReversed() ? row.cf.getSortedColumns() : row.cf.getReverseSortedColumns();
//        // printRow(cfs, new String(row.key.key.array()), cols);
//        String[] returnedColsNames = Iterables.toArray(Iterables.transform(cols, new Function<Cell, String>()
//        {
//            public String apply(Cell arg0)
//            {
//                return Util.string(arg0.name().toByteBuffer());
//            }
//        }), String.class);
//
//        assertTrue(
//                "Columns did not match. Expected: " + Arrays.toString(colNames) + " but got:"
//                        + Arrays.toString(returnedColsNames), Arrays.equals(colNames, returnedColsNames));
//        int i = 0;
//        for (Cell col : cols)
//        {
//            assertEquals(colNames[i++], Util.string(col.name().toByteBuffer()));
//        }
//    }
}
