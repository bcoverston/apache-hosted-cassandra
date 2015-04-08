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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterators;
import org.apache.cassandra.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.atoms.AtomIterator;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

@RunWith(OrderedJUnit4ClassRunner.class)
public class ColumnFamilyStoreTest
{
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String KEYSPACE2 = "ColumnFamilyStoreTest2";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_SUPER1 = "Super1";
    public static final String CF_SUPER6 = "Super6";
    public static final String CF_INDEX1 = "Indexed1";
    public static final String CF_INDEX2 = "Indexed2";
    public static final String CF_INDEX3 = "Indexed3";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX1, true),
                                    SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX2, false));
                                    // TODO: Fix superCFMD failing on legacy table creation. Seems to be applying composite comparator to partition key
                                    // SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER1, LongType.instance));
                                    // SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER6, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", LexicalUUIDType.instance, UTF8Type.instance),
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1),
                                    SchemaLoader.indexCFMD(KEYSPACE2, CF_INDEX1, true),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX2, true),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX3, true).gcGraceSeconds(0));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1).truncateBlocking();
        // Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_SUPER1).truncateBlocking();

        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX1).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX2).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX3).truncateBlocking();
    }

    @Test
    // create two sstables, and verify that we only deserialize data from the most recent one
    public void testTimeSortedQuery()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        new RowUpdateBuilder(cfs.metadata, 0, "key1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 1, "key1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();
        cfs.forceBlockingFlush();

        ((ClearableHistogram)cfs.metric.sstablesPerReadHistogram.cf).clear(); // resets counts
        Util.materializePartition(cfs, Util.dk("key1"));
        assertEquals(1, cfs.metric.sstablesPerReadHistogram.cf.getCount());
    }

    @Test
    public void testGetColumnWithWrongBF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        List<Mutation> rms = new LinkedList<>();
        rms.add(new RowUpdateBuilder(cfs.metadata, 0, "key1")
                .add("val", "asdf")
                .build());

        Util.writeColumnFamily(rms);

        List<SSTableReader> ssTables = keyspace.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        try (AtomIterator result = Util.readFullPartition(cfs, Util.dk("key2")))
        {
            assertEquals(0, Iterators.size(result));
        }
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD2);

        RowUpdateBuilder.deleteRow(cfs.metadata, FBUtilities.timestampMicros(), "key1").applyUnsafe();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                Row toCheck = Util.getSingleRow(cfs, Util.dk("key1"));
                Iterator<Cell> iter = toCheck.iterator();
                assert(Iterators.size(iter) == 0);
            }
        };

        reTest(cfs, r);
    }

    // TODO: Implement this once we have hooks to super columns available in CQL context
//    @Test
//    public void testDeleteSuperRowSticksAfterFlush() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName= CF_SUPER1;
//
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//
//        ByteBuffer scfName = ByteBufferUtil.bytes("SuperDuper");
//        DecoratedKey key = Util.dk("flush-resurrection");
//
//        // create an isolated sstable.
//        putColSuper(cfs, key, 0, ByteBufferUtil.bytes("val"), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes(1L), ByteBufferUtil.bytes("val1"));

//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(1L), ByteBufferUtil.bytes("val1"), 1),
//                new BufferCell(cellname(2L), ByteBufferUtil.bytes("val2"), 1),
//                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 1));
//        cfs.forceBlockingFlush();
//
//        // insert, don't flush.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1),
//                new BufferCell(cellname(5L), ByteBufferUtil.bytes("val5"), 1),
//                new BufferCell(cellname(6L), ByteBufferUtil.bytes("val6"), 1));
//
//        // verify insert.
//        final SlicePredicate sp = new SlicePredicate();
//        sp.setSlice_range(new SliceRange());
//        sp.getSlice_range().setCount(100);
//        sp.getSlice_range().setStart(ArrayUtils.EMPTY_BYTE_ARRAY);
//        sp.getSlice_range().setFinish(ArrayUtils.EMPTY_BYTE_ARRAY);
//
//        assertRowAndColCount(1, 6, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // delete
//        Mutation rm = new Mutation(keyspace.getName(), key.getKey());
//        rm.deleteRange(cfName, SuperColumns.startOf(scfName), SuperColumns.endOf(scfName), 2);
//        rm.applyUnsafe();
//
//        // verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // flush
//        cfs.forceBlockingFlush();
//
//        // re-verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // late insert.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(4L), ByteBufferUtil.bytes("val4"), 1L),
//                new BufferCell(cellname(7L), ByteBufferUtil.bytes("val7"), 1L));
//
//        // re-verify delete.
//        assertRowAndColCount(1, 0, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//
//        // make sure new writes are recognized.
//        putColsSuper(cfs, key, scfName,
//                new BufferCell(cellname(3L), ByteBufferUtil.bytes("val3"), 3),
//                new BufferCell(cellname(8L), ByteBufferUtil.bytes("val8"), 3),
//                new BufferCell(cellname(9L), ByteBufferUtil.bytes("val9"), 3));
//        assertRowAndColCount(1, 3, false, cfs.getRangeSlice(Util.range("f", "g"), null, ThriftValidation.asIFilter(sp, cfs.metadata, scfName), 100));
//    }

//    private static void assertRowAndColCount(int rowCount, int colCount, boolean isDeleted, Collection<Row> rows) throws CharacterCodingException
//    {
//        assert rows.size() == rowCount : "rowcount " + rows.size();
//        for (Row row : rows)
//        {
//            assert row.cf != null : "cf was null";
//            assert row.cf.getColumnCount() == colCount : "colcount " + row.cf.getColumnCount() + "|" + str(row.cf);
//            if (isDeleted)
//                assert row.cf.isMarkedForDelete() : "cf not marked for delete";
//        }
//    }
//
//    private static String str(ColumnFamily cf) throws CharacterCodingException
//    {
//        StringBuilder sb = new StringBuilder();
//        for (Cell col : cf.getSortedColumns())
//            sb.append(String.format("(%s,%s,%d),", ByteBufferUtil.string(col.name().toByteBuffer()), ByteBufferUtil.string(col.value()), col.timestamp()));
//        return sb.toString();
//    }

    @Test
    public void testDeleteStandardRowSticksAfterFlush() throws Throwable
    {
        // test to make sure flushing after a delete doesn't resurrect delted cols.
        String keyspaceName = KEYSPACE1;
        String cfName = CF_STANDARD1;
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer col = ByteBufferUtil.bytes("val");
        ByteBuffer val = ByteBufferUtil.bytes("val1");

        // insert
        ColumnDefinition newCol = new ColumnDefinition(cfs.metadata, ByteBufferUtil.bytes("val2"), AsciiType.instance, 1, ColumnDefinition.Kind.REGULAR);
        new RowUpdateBuilder(cfs.metadata, 0, "key1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "key2").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 2);

        // flush.
        cfs.forceBlockingFlush();

        // insert, don't flush
        new RowUpdateBuilder(cfs.metadata, 1, "key3").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 1, "key4").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // delete (from sstable and memtable)
        RowUpdateBuilder.deleteRow(cfs.metadata, 5, "key1").applyUnsafe();
        RowUpdateBuilder.deleteRow(cfs.metadata, 5, "key3").applyUnsafe();

        // verify delete
        assertRangeCount(cfs, col, val, 2);

        // flush
        cfs.forceBlockingFlush();

        // re-verify delete. // first breakage is right here because of CASSANDRA-1837.
        assertRangeCount(cfs, col, val, 2);

        // simulate a 'late' insertion that gets put in after the deletion. should get inserted, but fail on read.
        new RowUpdateBuilder(cfs.metadata, 2, "key1").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 2, "key3").add("val", "val1").build().applyUnsafe();

        // should still be nothing there because we deleted this row. 2nd breakage, but was undetected because of 1837.
        assertRangeCount(cfs, col, val, 2);

        // make sure that new writes are recognized.
        new RowUpdateBuilder(cfs.metadata, 10, "key5").add("val", "val1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 10, "key6").add("val", "val1").build().applyUnsafe();
        assertRangeCount(cfs, col, val, 4);

        // and it remains so after flush. (this wasn't failing before, but it's good to check.)
        cfs.forceBlockingFlush();
        assertRangeCount(cfs, col, val, 4);
    }

    @Test
    public void testBackupAfterFlush() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD1);
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key1")).add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, 0, ByteBufferUtil.bytes("key2")).add("val", "asdf").build().applyUnsafe();
        cfs.forceBlockingFlush();

        for (int version = 1; version <= 2; ++version)
        {
            Descriptor existing = new Descriptor(cfs.directories.getDirectoryForNewSSTables(), KEYSPACE2, CF_STANDARD1, version, Descriptor.Type.FINAL);
            Descriptor desc = new Descriptor(Directories.getBackupsDirectory(existing), KEYSPACE2, CF_STANDARD1, version, Descriptor.Type.FINAL);
            for (Component c : new Component[]{ Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.STATS })
                assertTrue("Cannot find backed-up file:" + desc.filenameFor(c), new File(desc.filenameFor(c)).exists());
        }
    }

    // TODO: Fix once we have working supercolumns in 8099
//    // CASSANDRA-3467.  the key here is that supercolumn and subcolumn comparators are different
//    @Test
//    public void testSliceByNamesCommandOnUUIDTypeSCF() throws Throwable
//    {
//        String keyspaceName = KEYSPACE1;
//        String cfName = CF_SUPER6;
//        ByteBuffer superColName = LexicalUUIDType.instance.fromString("a4ed3562-0e8e-4b41-bdfd-c45a2774682d");
//        Keyspace keyspace = Keyspace.open(keyspaceName);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
//        DecoratedKey key = Util.dk("slice-get-uuid-type");
//
//        // Insert a row with one supercolumn and multiple subcolumns
//        putColsSuper(cfs, key, superColName, new BufferCell(cellname("a"), ByteBufferUtil.bytes("A"), 1),
//                                             new BufferCell(cellname("b"), ByteBufferUtil.bytes("B"), 1));
//
//        // Get the entire supercolumn like normal
//        ColumnFamily cfGet = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
//        assertEquals(ByteBufferUtil.bytes("A"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfGet.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//
//        // Now do the SliceByNamesCommand on the supercolumn, passing both subcolumns in as columns to get
//        SortedSet<CellName> sliceColNames = new TreeSet<CellName>(cfs.metadata.comparator);
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a")));
//        sliceColNames.add(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b")));
//        SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(keyspaceName, key.getKey(), cfName, System.currentTimeMillis(), new NamesQueryFilter(sliceColNames));
//        ColumnFamily cfSliced = cmd.getRow(keyspace).cf;
//
//        // Make sure the slice returns the same as the straight get
//        assertEquals(ByteBufferUtil.bytes("A"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("a"))).value());
//        assertEquals(ByteBufferUtil.bytes("B"), cfSliced.getColumn(CellNames.compositeDense(superColName, ByteBufferUtil.bytes("b"))).value());
//    }

    // TODO: fix once SSTableSimpleWriter's back in
//    @Test
//    public void testRemoveUnfinishedCompactionLeftovers() throws Throwable
//    {
//        String ks = KEYSPACE1;
//        String cf = CF_STANDARD3; // should be empty
//
//        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
//        Directories dir = new Directories(cfmeta);
//        ByteBuffer key = bytes("key");
//
//        // 1st sstable
//        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(), cfmeta, StorageService.getPartitioner());
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        Map<Descriptor, Set<Component>> sstables = dir.sstableLister().list();
//        assertEquals(1, sstables.size());
//
//        Map.Entry<Descriptor, Set<Component>> sstableToOpen = sstables.entrySet().iterator().next();
//        final SSTableReader sstable1 = SSTableReader.open(sstableToOpen.getKey());
//
//        // simulate incomplete compaction
//        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                         cfmeta, StorageService.getPartitioner())
//        {
//            protected SSTableWriter getWriter()
//            {
//                MetadataCollector collector = new MetadataCollector(cfmeta.comparator);
//                collector.addAncestor(sstable1.descriptor.generation); // add ancestor from previously written sstable
//                return SSTableWriter.create(createDescriptor(directory, metadata.ksName, metadata.cfName, DatabaseDescriptor.getSSTableFormat()),
//                        0L,
//                        ActiveRepairService.UNREPAIRED_SSTABLE,
//                        metadata,
//                        DatabaseDescriptor.getPartitioner(),
//                        collector);
//            }
//        };
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        // should have 2 sstables now
//        sstables = dir.sstableLister().list();
//        assertEquals(2, sstables.size());
//
//        SSTableReader sstable2 = SSTableReader.open(sstable1.descriptor);
//        UUID compactionTaskID = SystemKeyspace.startCompaction(
//                Keyspace.open(ks).getColumnFamilyStore(cf),
//                Collections.singleton(sstable2));
//
//        Map<Integer, UUID> unfinishedCompaction = new HashMap<>();
//        unfinishedCompaction.put(sstable1.descriptor.generation, compactionTaskID);
//        ColumnFamilyStore.removeUnfinishedCompactionLeftovers(cfmeta, unfinishedCompaction);
//
//        // 2nd sstable should be removed (only 1st sstable exists in set of size 1)
//        sstables = dir.sstableLister().list();
//        assertEquals(1, sstables.size());
//        assertTrue(sstables.containsKey(sstable1.descriptor));
//
//        Map<Pair<String, String>, Map<Integer, UUID>> unfinished = SystemKeyspace.getUnfinishedCompactions();
//        assertTrue(unfinished.isEmpty());
//        sstable1.selfRef().release();
//        sstable2.selfRef().release();
//    }

    // TODO: Fix once SSTableSimpleWriter's back in
    // @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-6086">CASSANDRA-6086</a>
//    @Test
//    public void testFailedToRemoveUnfinishedCompactionLeftovers() throws Throwable
//    {
//        final String ks = KEYSPACE1;
//        final String cf = CF_STANDARD4; // should be empty
//
//        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
//        Directories dir = new Directories(cfmeta);
//        ByteBuffer key = bytes("key");
//
//        // Write SSTable generation 3 that has ancestors 1 and 2
//        final Set<Integer> ancestors = Sets.newHashSet(1, 2);
//        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                                cfmeta, StorageService.getPartitioner())
//        {
//            protected SSTableWriter getWriter()
//            {
//                MetadataCollector collector = new MetadataCollector(cfmeta.comparator);
//                for (int ancestor : ancestors)
//                    collector.addAncestor(ancestor);
//                String file = new Descriptor(directory, ks, cf, 3, Descriptor.Type.TEMP).filenameFor(Component.DATA);
//                return SSTableWriter.create(Descriptor.fromFilename(file),
//                        0L,
//                        ActiveRepairService.UNREPAIRED_SSTABLE,
//                        metadata,
//                        StorageService.getPartitioner(),
//                        collector);
//            }
//        };
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        Map<Descriptor, Set<Component>> sstables = dir.sstableLister().list();
//        assert sstables.size() == 1;
//
//        Map.Entry<Descriptor, Set<Component>> sstableToOpen = sstables.entrySet().iterator().next();
//        final SSTableReader sstable1 = SSTableReader.open(sstableToOpen.getKey());
//
//        // simulate we don't have generation in compaction_history
//        Map<Integer, UUID> unfinishedCompactions = new HashMap<>();
//        UUID compactionTaskID = UUID.randomUUID();
//        for (Integer ancestor : ancestors)
//            unfinishedCompactions.put(ancestor, compactionTaskID);
//        ColumnFamilyStore.removeUnfinishedCompactionLeftovers(cfmeta, unfinishedCompactions);
//
//        // SSTable should not be deleted
//        sstables = dir.sstableLister().list();
//        assert sstables.size() == 1;
//        assert sstables.containsKey(sstable1.descriptor);
//    }

    // TODO: Fix once SSTableSimpleWriter's back in
//    @Test
//    public void testLoadNewSSTablesAvoidsOverwrites() throws Throwable
//    {
//        String ks = KEYSPACE1;
//        String cf = CF_STANDARD1;
//        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(cf);
//        SSTableDeletingTask.waitForDeletions();
//
//        final CFMetaData cfmeta = Schema.instance.getCFMetaData(ks, cf);
//        Directories dir = new Directories(cfs.metadata);
//
//        // clear old SSTables (probably left by CFS.clearUnsafe() calls in other tests)
//        for (Map.Entry<Descriptor, Set<Component>> entry : dir.sstableLister().list().entrySet())
//        {
//            for (Component component : entry.getValue())
//            {
//                FileUtils.delete(entry.getKey().filenameFor(component));
//            }
//        }
//
//        // sanity check
//        int existingSSTables = dir.sstableLister().list().keySet().size();
//        assert existingSSTables == 0 : String.format("%d SSTables unexpectedly exist", existingSSTables);
//
//        ByteBuffer key = bytes("key");
//
//        SSTableSimpleWriter writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                                             cfmeta, StorageService.getPartitioner())
//        {
//            @Override
//            protected SSTableWriter getWriter()
//            {
//                // hack for reset generation
//                generation.set(0);
//                return super.getWriter();
//            }
//        };
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        writer = new SSTableSimpleWriter(dir.getDirectoryForNewSSTables(),
//                                         cfmeta, StorageService.getPartitioner());
//        writer.newRow(key);
//        writer.addColumn(bytes("col"), bytes("val"), 1);
//        writer.close();
//
//        Set<Integer> generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // we should have two generations: [1, 2]
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(1));
//        assertTrue(generations.contains(2));
//
//        assertEquals(0, cfs.getSSTables().size());
//
//        // start the generation counter at 1 again (other tests have incremented it already)
//        cfs.resetFileIndexGenerator();
//
//        boolean incrementalBackupsEnabled = DatabaseDescriptor.isIncrementalBackupsEnabled();
//        try
//        {
//            // avoid duplicate hardlinks to incremental backups
//            DatabaseDescriptor.setIncrementalBackupsEnabled(false);
//            cfs.loadNewSSTables();
//        }
//        finally
//        {
//            DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackupsEnabled);
//        }
//
//        assertEquals(2, cfs.getSSTables().size());
//        generations = new HashSet<>();
//        for (Descriptor descriptor : dir.sstableLister().list().keySet())
//            generations.add(descriptor.generation);
//
//        // normally they would get renamed to generations 1 and 2, but since those filenames already exist,
//        // they get skipped and we end up with generations 3 and 4
//        assertEquals(2, generations.size());
//        assertTrue(generations.contains(3));
//        assertTrue(generations.contains(4));
//    }

    public void reTest(ColumnFamilyStore cfs, Runnable verify) throws Exception
    {
        verify.run();
        cfs.forceBlockingFlush();
        verify.run();
    }

    private void assertRangeCount(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val, int count)
    {
        assertRangeCount(cfs, cfs.metadata.getColumnDefinition(col), val, count);
    }
    private void assertRangeCount(ColumnFamilyStore cfs, ColumnDefinition col, ByteBuffer val, int count)
    {
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addFilter(col, Operator.EQ, val)
                .build().executeLocally())
        {
            int found = 0;
            if (count != 0)
            {
                while (iter.hasNext())
                {
                    RowIterator ri = iter.next();
                    while (ri.hasNext())
                    {
                        Row r = ri.next();
                        if (r.getCell(col).value().equals(val))
                            ++found;
                    }
                }
            }
            assertEquals(count, found);
        }
    }
}
