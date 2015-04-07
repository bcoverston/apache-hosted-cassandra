package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CellTest
{
    private static final String KEYSPACE1 = "CellTest";
    private static final String CF_STANDARD1 = "Standard1";

    private CFMetaData cfm = SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testConflictingTypeEquality()
    {
        boolean[] tf = new boolean[]{ true, false };
        for (boolean lhs : tf)
        {
            for (boolean rhs : tf)
            {
                // don't test equality for both sides native, as this is based on CellName resolution
                if (lhs && rhs)
                    continue;
                Cell a = buildCell(cfm, "a", "a", 1, 1);
                Cell b = buildCell(cfm, "a", "a", 1, 0);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);

                a = deleted(cfm, "a", "a", 1, 1);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);
            }
        }
    }

    @Test
    public void testExpiringCellReconile()
    {
        // equal
        Assert.assertEquals(0, testExpiring("a", "a", 1, 1, null, null, null, null));

        // newer timestamp
        Assert.assertEquals(-1, testExpiring("a", "a", 2, 1, null, null, 1L, null));
        Assert.assertEquals(-1, testExpiring("a", "a", 2, 1, null, "b", 1L, 2));

        Assert.assertEquals(-1, testExpiring("a", "a", 1, 2, null, null, null, 1));
        Assert.assertEquals(1, testExpiring("a", "a", 1, 2, null, "b", null, 1));

        // newer value
        Assert.assertEquals(-1, testExpiring("a", "b", 2, 1, null, "a", null, null));
        Assert.assertEquals(-1, testExpiring("a", "b", 2, 1, null, "a", null, 2));
    }

    private int testExpiring(String n1, String v1, long t1, int et1, String n2, String v2, Long t2, Integer et2)
    {
        if (n2 == null)
            n2 = n1;
        if (v2 == null)
            v2 = v1;
        if (t2 == null)
            t2 = t1;
        if (et2 == null)
            et2 = et1;
        Cell c1 = buildCell(cfm, n1, v1, t1, et1);
        Cell c2 = buildCell(cfm, n2, v2, t2, et2);

        int now = FBUtilities.nowInSeconds();
        if (Cells.reconcile(c1, c2, now) == c1)
            return Cells.reconcile(c2, c1, now) == c1 ? -1 : 0;
        return Cells.reconcile(c2, c1, now) == c2 ? 1 : 0;
    }

    private Cell buildCell(CFMetaData cfm, String columnName, String value, long timestamp, int ttl)
    {
        ColumnDefinition cdef = cfm.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        LivenessInfo info = SimpleLivenessInfo.forUpdate(timestamp, ttl, FBUtilities.nowInSeconds(), cfm);
        return new TestCell(cdef, ByteBufferUtil.bytes(value), info);
    }

    private Cell deleted(CFMetaData cfm, String columnName, String value, int localDeletionTime, long timestamp)
    {
        ColumnDefinition cdef = cfm.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        LivenessInfo info = SimpleLivenessInfo.forDeletion(timestamp, localDeletionTime);
        return new TestCell(cdef, ByteBufferUtil.bytes(value), info);
    }

    class TestCell extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        public TestCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return false;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }
}
