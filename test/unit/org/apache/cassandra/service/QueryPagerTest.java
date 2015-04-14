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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.Cell;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.NamesPartitionFilter;
import org.apache.cassandra.db.filter.SlicePartitionFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.db.partitions.ReadPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.thrift.ColumnSlice;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.*;

@RunWith(OrderedJUnit4ClassRunner.class)
public class QueryPagerTest
{
    public static final String KEYSPACE1 = "QueryPagerTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                CFMetaData.compile("CREATE TABLE " + CF_CQL + " ("
                        + "k text,"
                        + "c text,"
                        + "v text,"
                        + "PRIMARY KEY (k, c))", KEYSPACE_CQL));
        addData();
    }


    private static String string(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void addData()
    {
        cfs().clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;

        // *
        // * Creates the following data:
        // *   k1: c1 ... cn
        // *   ...
        // *   ki: c1 ... cn
        // *
        for (int i = 0; i < nbKeys; i++)
        {

            for (int j = 0; j < nbCols; j++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs().metadata, FBUtilities.timestampMicros(), bytes("k"+i));
                builder.clustering("c"+j).add("val", "a").build().applyUnsafe();
            }
        }
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
    }

    private static List<ReadPartition> assertSize(DataIterator partitions, int expectedSize)
    {
        StringBuilder sb = new StringBuilder();
        int size = 0;


        List<ReadPartition> partitionList = new ArrayList<>();

        for (RowIterator rowIterator : Util.once(partitions))
        {
            ReadPartition partition = ReadPartition.create(rowIterator);
            sb.append(partition);

            partitionList.add(partition);

            size++;
        }

        assertEquals(sb.toString(), expectedSize, size);

        return partitionList;
    }

    private static ReadCommand namesQuery(String key, String... names)
    {
        SinglePartitionNamesReadBuilder builder = new SinglePartitionNamesReadBuilder(cfs(), Util.dk(key));

        for (String name : names)
        {
            builder.addClustering(name);
        }

        return builder.setPagingLimit(100).build();
    }

    private static SinglePartitionSliceCommand sliceQuery(String key, String start, String end, int count)
    {
        return sliceQuery(key, start, end, false, count);
    }

    private static SinglePartitionSliceCommand sliceQuery(String key, String start, String end, boolean reversed, int count)
    {

        ClusteringComparator cmp = cfs().getComparator();
        CFMetaData metadata = cfs().metadata;

        SlicePartitionFilter filter = new SlicePartitionFilter(metadata.partitionColumns(), Slices.with(cmp, Slice.make(cmp, cmp.make(start), cmp.make(end))), false);

        SinglePartitionSliceCommand command = new SinglePartitionSliceCommand(cfs().metadata, FBUtilities.nowInSeconds(), ColumnFilter.NONE, DataLimits.NONE, Util.dk(key), filter);

        return command;
    }


    private static ReadCommand rangeNamesQuery(String keyStart, String keyEnd, int count, String... names)
    {
        PartitionRangeReadBuilder builder = new PartitionRangeReadBuilder(cfs());

        for (String name : names)
            builder.addClustering(name);

        builder.setKeyBounds(bytes(keyStart), bytes(keyEnd))
                .setPagingLimit(count);

        return builder.build();
    }

    private static ReadCommand rangeSliceQuery(String keyStart, String keyEnd, int count, String start, String end)
    {
        PartitionRangeReadBuilder builder = new PartitionRangeReadBuilder(cfs());

        builder.setKeyBounds(bytes(keyStart), bytes(keyEnd))
                .setClusteringLowerBound(true, bytes(start))
                .setClusteringUpperBound(true, bytes(end))
                .setPagingLimit(count);

        return builder.build();
    }

    private static void assertRow(ReadPartition r, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(r, key, bbs);
    }

    private static void assertRow(ReadPartition partition, String key, ByteBuffer... names)
    {
        assertEquals(key, string(partition.partitionKey().getKey()));
        assertFalse(partition.isEmpty());
        int i = 0;
        for (Row row : Util.once(partition.iterator()))
        {
            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match "+string(expected)+" vs "+string(row.clustering().get(0)), expected, row.clustering().get(0));
        }
    }

    @Test
    public void namesQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(namesQuery("k0", "c1", "c5", "c7", "c8"));

        assertFalse(pager.isExhausted());
        DataIterator page = pager.fetchPage(4);

        List<ReadPartition> partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c1", "c5", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(sliceQuery("k0", "c1", "c8", 10));

        DataIterator page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        List<ReadPartition> partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c1", "c2", "c3");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c4", "c5", "c6");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c7", "c8");

        assertTrue(pager.isExhausted());
    }


    @Test
    public void reversedSliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(sliceQuery("k0", "c8", "c1", true, 10));

        DataIterator page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        List<ReadPartition> partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c6", "c7", "c8");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c3", "c4", "c5");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partition = assertSize(page, 1);
        assertRow(partition.get(0), "k0", "c1", "c2");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void multiQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(
                new SinglePartitionReadCommand.Group(new ArrayList<SinglePartitionReadCommand<?>>()
            {{
                add(sliceQuery("k1", "c2", "c6", 10));
                add(sliceQuery("k4", "c3", "c5", 10));
            }}, DataLimits.NONE));

        DataIterator page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        List<ReadPartition> partition = assertSize(page, 1);
        assertRow(partition.get(0), "k1", "c2", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(4);
        partition = assertSize(page , 2);
        assertRow(partition.get(0), "k1", "c5", "c6");
        assertRow(partition.get(1), "k4", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partition = assertSize(page, 1);
        assertRow(partition.get(0), "k4", "c5");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeNamesQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(rangeNamesQuery("k0", "k5", 100, "c1", "c4", "c8"));

        DataIterator page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        List<ReadPartition> partitions = assertSize(page, 3);
        for (int i = 1; i <= 3; i++)
            assertRow(partitions.get(i-1), "k" + i, "c1", "c4", "c8");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        partitions = assertSize(page, 2);
        for (int i = 4; i <= 5; i++)
            assertRow(partitions.get(i-4), "k" + i, "c1", "c4", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(rangeSliceQuery("k1", "k5", 100, "c1", "c7"));

        DataIterator page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        List<ReadPartition> partitions = assertSize(page, 1);
        assertRow(partitions.get(0), "k2", "c1", "c2", "c3", "c4", "c5");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(4);
        partitions = assertSize(page, 2);
        assertRow(partitions.get(0), "k2", "c6", "c7");
        assertRow(partitions.get(1), "k3", "c1", "c2");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(6);
        partitions = assertSize(page, 2);
        assertRow(partitions.get(0), "k3", "c3", "c4", "c5", "c6", "c7");
        assertRow(partitions.get(1), "k4", "c1");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        partitions = assertSize(page, 1);
        assertRow(partitions.get(0), "k4", "c2", "c3", "c4", "c5", "c6");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        partitions = assertSize(page, 2);
        assertRow(partitions.get(0), "k4", "c7");
        assertRow(partitions.get(1), "k5", "c1", "c2", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        partitions = assertSize(page, 1);
        assertRow(partitions.get(0), "k5", "c5", "c6", "c7");

        assertTrue(pager.isExhausted());
    }


    @Test
    public void SliceQueryWithTombstoneTest() throws Exception
    {
        // Testing for the bug of #6748
        String keyspace = "cql_keyspace";
        String table = "table2";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", keyspace, table, 0, i));

        ReadCommand command = SinglePartitionSliceCommand.create(cfs.metadata, FBUtilities.nowInSeconds(), Util.dk("k0"), Slice.ALL);

        QueryPager pager = QueryPagers.localPager(command);

        for (int i = 0; i < 5; i++)
        {
            DataIterator page = pager.fetchPage(1);
            List<ReadPartition> partitions = assertSize(page, 1);
            // The only live cell we should have each time is the row marker
            assertRow(partitions.get(0), "k0", "c" + i);
        }
    }
}
