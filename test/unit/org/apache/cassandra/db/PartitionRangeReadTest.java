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

import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collection;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.collect.Iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.*;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class PartitionRangeReadTest
{
    public static final String KEYSPACE1 = "PartitionRangeReadTest1";
    public static final String KEYSPACE2 = "PartitionRangeReadTest2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARDINT = "StandardInteger1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.denseCFMD(KEYSPACE1, CF_STANDARDINT, IntegerType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1));
    }

    @Test
    public void testInclusiveBounds()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1);
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1"))
                .clustering("cc1")
                .add("val", "asdf").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key2"))
                .clustering("cc2")
                .add("val", "asdf").build().applyUnsafe();

        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
             .setClusteringLowerBound(true, ByteBufferUtil.bytes("cc1"))
             .setClusteringUpperBound(true, ByteBufferUtil.bytes("cc2"))
             .executeLocally())
        {
            assertEquals(2, Iterators.size(iter));
        }
    }

    @Test
    public void testPartitionRangeReadOldMetadata() throws Throwable
    {
        String keyspaceName = KEYSPACE1;
        String cfName= CF_STANDARD1;
        String key = "slice-name-old-metadata";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        // Create a cell a 'high timestamp'
        new RowUpdateBuilder(cfs.metadata, 20, key).add("val", "val1").build().applyUnsafe();
        cfs.forceBlockingFlush();

        // Nuke the metadata and reload that sstable
        Collection<SSTableReader> ssTables = cfs.getSSTables();
        assertEquals(1, ssTables.size());
        cfs.clearUnsafe();
        assertEquals(0, cfs.getSSTables().size());

        new File(ssTables.iterator().next().descriptor.filenameFor(Component.STATS)).delete();

        // TODO: Fix, failing on new assertion in SSTableReader.open, header comes back null and storeRows for version 'la' is true.
        cfs.loadNewSSTables();

        // Add another cell with a lower timestamp
        new RowUpdateBuilder(cfs.metadata, 10, key).add("val", "val2").build().applyUnsafe();

        // Test fetching the cell by name returns the first cell
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
             .executeLocally())
        {
            assertTrue(iter.next().next().getCell(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"))).value().equals(ByteBufferUtil.bytes("val1")));
        }

        Keyspace.clear(KEYSPACE1); // CASSANDRA-7195
    }


    @Test
    public void testCassandra6778() throws CharacterCodingException
    {
        String cfname = CF_STANDARDINT;
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();

        ByteBuffer col = ByteBufferUtil.bytes("val");
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(col);

        // insert two columns that represent the same integer but have different binary forms (the
        // second one is padded with extra zeros)
        new RowUpdateBuilder(cfs.metadata, 0, "k1")
                .clustering(new BigInteger(new byte[]{1}))
                .add("val", "val1")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 1, "k1")
                .clustering(new BigInteger(new byte[]{0, 0, 1}))
                .add("val", "val2")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        // fetch by the first column name; we should get the second version of the column value
        try (DataIterator iter = new SinglePartitionNamesReadBuilder(cfs, FBUtilities.nowInSeconds(), Util.dk("k1"))
             .addClustering(new BigInteger(new byte[]{1}))
             .executeLocally())
        {
            assertTrue(iter.hasNext());
            RowIterator ri = iter.next();
            assertTrue(ri.hasNext());
            Row r = ri.next();
            assertTrue(r.getCell(cDef).value().equals(ByteBufferUtil.bytes("val2")));
        }

        // fetch by the second column name; we should get the second version of the column value
        try (DataIterator iter = new SinglePartitionNamesReadBuilder(cfs, FBUtilities.nowInSeconds(), Util.dk("k1"))
             .addClustering(new BigInteger(new byte[]{0, 0, 1}))
             .executeLocally())
        {
            assertTrue(iter.hasNext());
            RowIterator ri = iter.next();
            assertTrue(ri.hasNext());
            Row r = ri.next();
            assertTrue(r.getCell(cDef).value().equals(ByteBufferUtil.bytes("val2")));
        }
    }

    @Test
    public void testRangeSliceInclusionExclusion() throws Throwable
    {
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        cfs.clearUnsafe();

        for (int i = 0; i < 10; ++i)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 10, String.valueOf(i));
            builder.add("val", String.valueOf(i));
            builder.build().applyUnsafe();
        }

        cfs.forceBlockingFlush();

        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));

        // Start and end inclusive
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs)
             .setKeyBounds(ByteBufferUtil.bytes("2"), ByteBufferUtil.bytes("7"))
             .setRangeType(PartitionRangeReadBuilder.RangeType.Inclusive)
             .executeLocally())
        {
            List<Row> rows = AbstractReadCommandBuilder.getRowList(iter);
            assertEquals(6, rows.size());
            assertTrue(rows.get(0).getCell(cDef).value().equals(ByteBufferUtil.bytes("2")));
            assertTrue(rows.get(rows.size() - 1).getCell(cDef).value().equals(ByteBufferUtil.bytes("7")));
        }

        // Start and end excluded
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs)
             .setKeyBounds(ByteBufferUtil.bytes("2"), ByteBufferUtil.bytes("7"))
             .setRangeType(PartitionRangeReadBuilder.RangeType.Exclusive)
             .executeLocally())
        {
            List<Row> rows = AbstractReadCommandBuilder.getRowList(iter);
            assertEquals(4, rows.size());
            assertTrue(rows.get(0).getCell(cDef).value().equals(ByteBufferUtil.bytes("3")));
            assertTrue(rows.get(rows.size() - 1).getCell(cDef).value().equals(ByteBufferUtil.bytes("6")));
        }

        // Start excluded, end included
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs)
             .setKeyBounds(ByteBufferUtil.bytes("2"), ByteBufferUtil.bytes("7"))
             .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
             .executeLocally())
        {
            List<Row> rows = AbstractReadCommandBuilder.getRowList(iter);
            assertEquals(5, rows.size());
            assertTrue(rows.get(0).getCell(cDef).value().equals(ByteBufferUtil.bytes("3")));
            assertTrue(rows.get(rows.size() - 1).getCell(cDef).value().equals(ByteBufferUtil.bytes("7")));
        }

        // Start included, end excluded
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs)
             .setKeyBounds(ByteBufferUtil.bytes("2"), ByteBufferUtil.bytes("7"))
             .setRangeType(PartitionRangeReadBuilder.RangeType.ReverseRange)
             .executeLocally())
        {
            List<Row> rows = AbstractReadCommandBuilder.getRowList(iter);
            assertEquals(5, rows.size());
            assertTrue(rows.get(0).getCell(cDef).value().equals(ByteBufferUtil.bytes("2")));
            assertTrue(rows.get(rows.size() - 1).getCell(cDef).value().equals(ByteBufferUtil.bytes("6")));
        }
    }

        // TODO: Port or remove, depending on what DataLimits.thriftLimits (per cell) looks like
//    @Test
//    public void testRangeSliceColumnsLimit() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        Cell[] cols = new Cell[5];
//        for (int i = 0; i < 5; i++)
//            cols[i] = column("c" + i, "value", 1);
//
//        putColsStandard(cfs, Util.dk("a"), cols[0], cols[1], cols[2], cols[3], cols[4]);
//        putColsStandard(cfs, Util.dk("b"), cols[0], cols[1]);
//        putColsStandard(cfs, Util.dk("c"), cols[0], cols[1], cols[2], cols[3]);
//        cfs.forceBlockingFlush();
//
//        SlicePredicate sp = new SlicePredicate();
//        sp.setSlice_range(new SliceRange());
//        sp.getSlice_range().setCount(1);
//        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              3,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            3);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              5,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            5);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              8,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            8);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              10,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            10);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              100,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            11);
//
//        // Check that when querying by name, we always include all names for a
//        // gien row even if it means returning more columns than requested (this is necesseray for CQL)
//        sp = new SlicePredicate();
//        sp.setColumn_names(Arrays.asList(
//            ByteBufferUtil.bytes("c0"),
//            ByteBufferUtil.bytes("c1"),
//            ByteBufferUtil.bytes("c2")
//        ));
//
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              1,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            3);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              4,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            5);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              5,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            5);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              6,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            8);
//        assertTotalColCount(cfs.getRangeSlice(Util.range("", ""),
//                                              null,
//                                              ThriftValidation.asIFilter(sp, cfs.metadata, null),
//                                              100,
//                                              System.currentTimeMillis(),
//                                              true,
//                                              false),
//                            8);
//    }

    // TODO: Port or remove, depending on what DataLimits.thriftLimits (per cell) looks like
//    @Test
//    public void testRangeSlicePaging() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_STANDARD1;
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        cfs.clearUnsafe();
//
//        Cell[] cols = new Cell[4];
//        for (int i = 0; i < 4; i++)
//            cols[i] = column("c" + i, "value", 1);
//
//        DecoratedKey ka = Util.dk("a");
//        DecoratedKey kb = Util.dk("b");
//        DecoratedKey kc = Util.dk("c");
//
//        RowPosition min = Util.rp("");
//
//        putColsStandard(cfs, ka, cols[0], cols[1], cols[2], cols[3]);
//        putColsStandard(cfs, kb, cols[0], cols[1], cols[2]);
//        putColsStandard(cfs, kc, cols[0], cols[1], cols[2], cols[3]);
//        cfs.forceBlockingFlush();
//
//        SlicePredicate sp = new SlicePredicate();
//        sp.setSlice_range(new SliceRange());
//        sp.getSlice_range().setCount(1);
//        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        Collection<Row> rows;
//        Row row, row1, row2;
//        IDiskAtomFilter filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
//
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(Util.range("", ""), filter, null, 3, true, true, System.currentTimeMillis()));
//        assert rows.size() == 1 : "Expected 1 row, got " + toString(rows);
//        row = rows.iterator().next();
//        assertColumnNames(row, "c0", "c1", "c2");
//
//        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
//        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(ka, min), filter, null, 3, true, true, System.currentTimeMillis()));
//        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
//        Iterator<Row> iter = rows.iterator();
//        row1 = iter.next();
//        row2 = iter.next();
//        assertColumnNames(row1, "c2", "c3");
//        assertColumnNames(row2, "c0");
//
//        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c0")));
//        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(row2.key, min), filter, null, 3, true, true, System.currentTimeMillis()));
//        assert rows.size() == 1 : "Expected 1 row, got " + toString(rows);
//        row = rows.iterator().next();
//        assertColumnNames(row, "c0", "c1", "c2");
//
//        sp.getSlice_range().setStart(ByteBufferUtil.getArray(ByteBufferUtil.bytes("c2")));
//        filter = ThriftValidation.asIFilter(sp, cfs.metadata, null);
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(row.key, min), filter, null, 3, true, true, System.currentTimeMillis()));
//        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
//        iter = rows.iterator();
//        row1 = iter.next();
//        row2 = iter.next();
//        assertColumnNames(row1, "c2");
//        assertColumnNames(row2, "c0", "c1");
//
//        // Paging within bounds
//        SliceQueryFilter sf = new SliceQueryFilter(cellname("c1"),
//                                                   cellname("c2"),
//                                                   false,
//                                                   0);
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(ka, kc), sf, cellname("c2"), cellname("c1"), null, 2, true, System.currentTimeMillis()));
//        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
//        iter = rows.iterator();
//        row1 = iter.next();
//        row2 = iter.next();
//        assertColumnNames(row1, "c2");
//        assertColumnNames(row2, "c1");
//
//        rows = cfs.getRangeSlice(cfs.makeExtendedFilter(new Bounds<RowPosition>(kb, kc), sf, cellname("c1"), cellname("c1"), null, 10, true, System.currentTimeMillis()));
//        assert rows.size() == 2 : "Expected 2 rows, got " + toString(rows);
//        iter = rows.iterator();
//        row1 = iter.next();
//        row2 = iter.next();
//        assertColumnNames(row1, "c1", "c2");
//        assertColumnNames(row2, "c1");
//    }
}

