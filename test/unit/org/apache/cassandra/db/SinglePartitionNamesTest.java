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

import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.*;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.atoms.Row;
import org.apache.cassandra.db.atoms.RowIterator;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.partitions.DataIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class SinglePartitionNamesTest
{
    public static final String KEYSPACE1 = "ColumnFamilyStoreTest1";
    public static final String CF_STANDARDINT = "StandardInteger1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.denseCFMD(KEYSPACE1, CF_STANDARDINT, IntegerType.instance));
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
}

