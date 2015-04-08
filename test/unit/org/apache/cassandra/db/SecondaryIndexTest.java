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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Iterators;

import static org.junit.Assert.*;

import org.apache.cassandra.*;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SecondaryIndexTest
{
    public static final String KEYSPACE1 = "SecondaryIndexTest1";
    public static final String KEYSPACE2 = "SecondaryIndexTest2";
    public static final String KEYSPACE3 = "SecondaryIndexTest3";
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
                                    SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX1, true),
                                    SchemaLoader.indexCFMD(KEYSPACE1, CF_INDEX2, false));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.indexCFMD(KEYSPACE2, CF_INDEX1, true),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX2, true),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE2, CF_INDEX3, true).gcGraceSeconds(0));
        SchemaLoader.createKeyspace(KEYSPACE3,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(5),
                                    SchemaLoader.indexCFMD(KEYSPACE3, CF_INDEX1, true));
    }

    @Before
    public void truncateCFS()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1).truncateBlocking();

        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX1).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX2).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_INDEX3).truncateBlocking();

        Keyspace.open(KEYSPACE3).getColumnFamilyStore(CF_INDEX1).truncateBlocking();
    }

    @Test
    public void testIndexScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1);

        new RowUpdateBuilder(cfs.metadata, 0, "k1").add("birthdate", 1L).add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k2").add("birthdate", 2L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k3").add("birthdate", 1L).add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "k4aaaa").add("birthdate", 3L).add("notbirthdate", 2L).build().applyUnsafe();

        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ByteBuffer nbBB = ByteBufferUtil.bytes("notbirthdate");
        ColumnDefinition bDef = cfs.metadata.getColumnDefinition(bBB);
        ColumnDefinition nbDef = cfs.metadata.getColumnDefinition(nbBB);

        // basic single-expression query
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k3"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addColumn(bBB).executeLocally())
        {
            RowIterator ri = iter.next();
            Row r = ri.next();
            // k2
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri2 = iter.next();
            r = ri2.next();
            // k3
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(1L));

            assert !ri.hasNext();
        }

        // 2 columns, 3 results
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .executeLocally())
        {
            RowIterator ri = iter.next();
            Row r = ri.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(2L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri2 = iter.next();
            r = ri2.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(1L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));

            RowIterator ri3 = iter.next();
            r = ri3.next();
            assert r.getCell(bDef).value().equals(ByteBufferUtil.bytes(3L));
            assert r.getCell(nbDef).value().equals(ByteBufferUtil.bytes(2L));
            assert !ri.hasNext();
        }

        // Verify getIndexSearchers finds the data for our rc
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k3"))
                .addColumn(bBB)
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(1L)).build();
        List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersFor(rc);
        assertEquals(searchers.size(), 1);
        try (PartitionIterator pi = searchers.get(0).search(rc))
        {
            assert(pi.hasNext());
        }

        // Verify gt on idx scan
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.GT, ByteBufferUtil.bytes(1L))
                .executeLocally())
        {
            int rowCount = 0;
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                while (ri.hasNext())
                {
                    ++rowCount;
                    assert ByteBufferUtil.toLong(ri.next().getCell(bDef).value()) > 1L;
                }
            }
            assertEquals(2, rowCount);
        }

        // Filter on non-indexed, LT comparison
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.LT, ByteBufferUtil.bytes(2L))
                .executeLocally())
        {
            assertFalse(iter.hasNext());
        }

        // Hit on primary, fail on non-indexed filter
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setKeyBounds(ByteBufferUtil.bytes("k1"), ByteBufferUtil.bytes("k4aaaa"))
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(1L))
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.NEQ, ByteBufferUtil.bytes(2L))
                .executeLocally())
        {
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testLargeScan()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ByteBuffer nbBB = ByteBufferUtil.bytes("notbirthdate");

        for (int i = 0; i < 100; i++)
        {
            new RowUpdateBuilder(cfs.metadata, FBUtilities.timestampMicros(), "key" + i)
                    .add("birthdate", 34L)
                    .add("notbirthdate", ByteBufferUtil.bytes((long) (i % 2)))
                    .build()
                    .applyUnsafe();
        }

        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .addFilter(cfs.metadata.getColumnDefinition(bBB), Operator.EQ, ByteBufferUtil.bytes(34L))
                .addFilter(cfs.metadata.getColumnDefinition(nbBB), Operator.EQ, ByteBufferUtil.bytes(1L))
                .executeLocally())
        {
            Set<DecoratedKey> keys = new HashSet<>();
            int rowCount = 0;
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                keys.add(ri.partitionKey());
                while (ri.hasNext())
                {
                    ri.next();
                    ++rowCount;
                }
            }
            // extra check that there are no duplicate results -- see https://issues.apache.org/jira/browse/CASSANDRA-2406
            assertEquals(rowCount, keys.size());
            assertEquals(50, rowCount);
        }
    }

    @Test
    public void testIndexDeletions() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE3).getColumnFamilyStore(CF_INDEX1);
        ByteBuffer bBB = ByteBufferUtil.bytes("birthdate");
        ColumnDefinition bDef = cfs.metadata.getColumnDefinition(bBB);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // Confirm addition works
        new RowUpdateBuilder(cfs.metadata, 0, "k1").add("birthdate", 1L).build().applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(1L));

        // delete the column directly
        RowUpdateBuilder.deleteRow(cfs.metadata, 1, "k1").applyUnsafe();
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(1L));

        // verify that it's not being indexed under any other value either
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());

        // resurrect w/ a newer timestamp
        new RowUpdateBuilder(cfs.metadata, 2, "k1").add("birthdate", 1L).build().apply();;
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(1L));

        // verify that row and delete w/ older timestamp does nothing
        RowUpdateBuilder.deleteRow(cfs.metadata, 1, "k1").applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(1L));

        // similarly, column delete w/ older timestamp should do nothing
        new RowUpdateBuilder(cfs.metadata, 1, "k1").delete(bDef).build().applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(1L));

        // delete the entire row (w/ newer timestamp this time)
        RowUpdateBuilder.deleteRow(cfs.metadata, 3, "k1").applyUnsafe();
        rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());

        // make sure obsolete mutations don't generate an index entry
        new RowUpdateBuilder(cfs.metadata, 3, "k1").add("birthdate", 1L).build().apply();;
        rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds()).build();
        assertEquals(0, cfs.indexManager.getIndexSearchersFor(rc).size());
    }

    @Test
    public void testIndexUpdate() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX1);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value, test that the index query fetches the new version
        new RowUpdateBuilder(cfs.metadata, 1, "testIndexUpdate").add("birthdate", 100L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 2, "testIndexUpdate").add("birthdate", 200L).build().applyUnsafe();

        // Confirm old version fetch fails
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(100L));

        // Confirm new works
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(200L));

        // update the birthdate value with an OLDER timestamp, and test that the index ignores this
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(300L));
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(200L));
    }

    @Test
    public void testIndexUpdateOverwritingExpiringColumns() throws Exception
    {
        // see CASSANDRA-7268
        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX1);
        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the birthdate value with an expiring column
        new RowUpdateBuilder(cfs.metadata, 1, 500, "K100").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(100L));

        // requires a 1s sleep because we calculate local expiry time as (now() / 1000) + ttl
        TimeUnit.SECONDS.sleep(1);

        // now overwrite with the same name/value/ttl, but the local expiry time will be different
        new RowUpdateBuilder(cfs.metadata, 1, 500, "K100").add("birthdate", 100L).build().applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(100L));

        // check that modifying the indexed value using the same timestamp behaves as expected
        new RowUpdateBuilder(cfs.metadata, 1, 500, "K101").add("birthdate", 101L).build().applyUnsafe();
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(101L));

        TimeUnit.SECONDS.sleep(1);

        new RowUpdateBuilder(cfs.metadata, 1, 500, "K101").add("birthdate", 102L).build().applyUnsafe();
        // Confirm 101 is gone
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(101L));

        // Confirm 102 is there
        assertIndexedOne(cfs, col, ByteBufferUtil.bytes(102L));
    }

    @Test
    public void testDeleteOfInconsistentValuesInKeysIndex() throws Exception
    {
        String keySpace = KEYSPACE2;
        String cfName = CF_INDEX1;

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer col = ByteBufferUtil.bytes("birthdate");

        // create a row and update the "birthdate" value
        new RowUpdateBuilder(cfs.metadata, 1, "k1").add("birthdate", 1L).build().applyUnsafe();

        // force a flush, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(cfName).forceBlockingFlush();

        // now apply another update, but force the index update to be skipped
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 2, "k1").add("birthdate", 2L).build(), true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(1L));
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(2L));

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 3, "k1").add("birthdate", 1L).build(), true, false);
        // TODO: This is failing - figure out why the above insert is updating the index
        assertIndexedNone(cfs, col, ByteBufferUtil.bytes(1L));
    }

    @Test
    public void testDeleteOfInconsistentValuesFromCompositeIndex() throws Exception
    {
        String keySpace = KEYSPACE2;
        String cfName = CF_INDEX2;

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer col = ByteBufferUtil.bytes("col1");

        ByteBuffer val1 = ByteBufferUtil.bytes("v1");
        ByteBuffer val2 = ByteBufferUtil.bytes("v2");

        // create a row and update the author value
        new RowUpdateBuilder(cfs.metadata, 0, rowKey).add("col1", "v1").build().applyUnsafe();

        // test that the index query fetches this version
        assertIndexedOne(cfs, col, val1);

        // force a flush and retry the query, so our index isn't being read from a memtable
        keyspace.getColumnFamilyStore(cfName).forceBlockingFlush();
        assertIndexedOne(cfs, col, val1);

        // now apply another update, but force the index update to be skipped
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 1, rowKey).add("col1", "v2").build(), true, false);

        // Now searching the index for either the old or new value should return 0 rows
        // because the new value was not indexed and the old value should be ignored
        // (and in fact purged from the index cf).
        // first check for the old value
        assertIndexedNone(cfs, col, val1);
        assertIndexedNone(cfs, col, val2);

        // now, reset back to the original value, still skipping the index update, to
        // make sure the value was expunged from the index when it was discovered to be inconsistent
        // TODO: Figure out why this is re-inserting
        keyspace.apply(new RowUpdateBuilder(cfs.metadata, 2, rowKey).add("col1", "v1").build(), true, false);
        // assertIndexedNone(cfs, col, val1);
    }

    // See CASSANDRA-6098
    @Test
    public void testDeleteCompositeIndex() throws Exception
    {
        String keySpace = KEYSPACE2;
        String cfName = CF_INDEX3; // has gcGrace 0

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer rowKey = ByteBufferUtil.bytes("k1");
        ByteBuffer clusterKey = ByteBufferUtil.bytes("ck1");
        ByteBuffer colName = ByteBufferUtil.bytes("col1");

        ByteBuffer val1 = ByteBufferUtil.bytes("v2");

        // Insert indexed value.
        new RowUpdateBuilder(cfs.metadata, 1, rowKey).add(cfs.metadata.getColumnDefinition(colName), val1).build().applyUnsafe();

        // Now delete the value
        RowUpdateBuilder.deleteRow(cfs.metadata, 2, rowKey).applyUnsafe();

        // We want the data to be gcable, but even if gcGrace == 0, we still need to wait 1 second
        // since we won't gc on a tie.
        try { Thread.sleep(1000); } catch (Exception e) {}

        // Read the index and we check we do get no value (and no NPE)
        // Note: the index will return the entry because it hasn't been deleted (we
        // haven't read yet nor compacted) but the data read itself will return null
        assertIndexedNone(cfs, colName, val1);
    }

    // See CASSANDRA-2628
    @Test
    public void testIndexScanWithLimitOne()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_INDEX1);
        Mutation rm;

        new RowUpdateBuilder(cfs.metadata, 0, "kk1").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk1").add("notbirthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk2").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk2").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk3").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk3").add("notbirthdate", 2L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk4").add("birthdate", 1L).build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, 0, "kk4").add("notbirthdate", 2L).build().applyUnsafe();

        // basic single-expression query, limit 1
        try (DataIterator iter = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
             .addFilter(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate")), Operator.EQ, ByteBufferUtil.bytes(1L))
             .addFilter(cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("notbirthdate")), Operator.GT, ByteBufferUtil.bytes(1L))
             .setCQLLimit(1)
             .executeLocally())
        {
            int legitRows = 0;
            // We get back a RowIterator for each partitionKey but all but 1 should be empty
            while (iter.hasNext())
            {
                RowIterator ri = iter.next();
                while (ri.hasNext())
                {
                    ++legitRows;
                    ri.next();
                }
            }
            assertEquals(1, legitRows);
        }
    }

    @Test
    public void testIndexCreate() throws IOException, InterruptedException, ExecutionException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX2);

        // create a row and update the birthdate value, test that the index query fetches the new version
        DecoratedKey dk = Util.dk("k1");
        new RowUpdateBuilder(cfs.metadata, 0, "k1").add("birthdate", 1L).build().applyUnsafe();

        ColumnDefinition old = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate"));
        old.setIndex("birthdate_index", IndexType.COMPOSITES, Collections.EMPTY_MAP);
        Future<?> future = cfs.indexManager.addIndexedColumn(old);
        future.get();
        // we had a bug (CASSANDRA-2244) where index would get created but not flushed -- check for that
        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("birthdate"));
        assert cfs.indexManager.getIndexForColumn(cDef).getIndexCfs().getSSTables().size() > 0;

        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), ByteBufferUtil.bytes(1L));

        // validate that drop clears it out & rebuild works (CASSANDRA-2320)
        SecondaryIndex indexedCfs = cfs.indexManager.getIndexForColumn(cDef);
        cfs.indexManager.removeIndexedColumn(ByteBufferUtil.bytes("birthdate"));
        assert !indexedCfs.isIndexBuilt(ByteBufferUtil.bytes("birthdate"));

        // rebuild & re-query
        future = cfs.indexManager.addIndexedColumn(cDef);
        future.get();
        assertIndexedOne(cfs, ByteBufferUtil.bytes("birthdate"), ByteBufferUtil.bytes(1L));
    }

    // TODO: Implement once KeysSearcher is ported
//    @Test
//    public void testKeysSearcher() throws Exception
//    {
//        //  Create secondary index and flush to disk
//        Keyspace keyspace = Keyspace.open(KEYSPACE1);
//        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_INDEX1);
//
//        for (int i = 0; i < 10; i++)
//        {
//            new RowUpdateBuilder(cfs.metadata, 0, "k" + i).add("val", String.valueOf(i)).build().applyUnsafe();
//        }
//        cfs.forceBlockingFlush();
//
//        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, LongType.instance.decompose(1L));
//        // explicitly tell to the KeysSearcher to use column limiting for rowsPerQuery to trigger bogus columnsRead--; (CASSANDRA-3996)
//        List<Row> rows = cfs.search(store.makeExtendedFilter(Util.range("", ""), new IdentityQueryFilter(), Arrays.asList(expr), 10, true, false, System.currentTimeMillis()));
//
//        assert rows.size() == 10;
//    }

    private void assertIndexedNone(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val)
    {
        assertIndexedCount(cfs, col, val, 0);
    }
    private void assertIndexedOne(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val)
    {
        assertIndexedCount(cfs, col, val, 1);
    }
    private void assertIndexedCount(ColumnFamilyStore cfs, ByteBuffer col, ByteBuffer val, int count)
    {
        ReadCommand rc = new PartitionRangeReadBuilder(cfs, FBUtilities.nowInSeconds())
                .setRangeType(PartitionRangeReadBuilder.RangeType.Range)
                .addFilter(cfs.metadata.getColumnDefinition(col), Operator.EQ, val)
                .build();

        List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersFor(rc);
        if (count != 0)
            assertTrue(searchers.size() > 0);

        try (PartitionIterator iter = searchers.get(0).search(rc))
        {
            assertEquals(count, Iterators.size(iter));
        }
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
