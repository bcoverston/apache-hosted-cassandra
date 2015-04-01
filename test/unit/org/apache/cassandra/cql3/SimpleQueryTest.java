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
package org.apache.cassandra.cql3;

import java.util.*;
import org.junit.Test;

import static junit.framework.Assert.*;

public class SimpleQueryTest extends CQLTester
{
    @Test
    public void testTableWithoutClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v1 int, v2 text);");

        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "first", 1, "value1");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "second", 2, "value2");
        execute("INSERT INTO %s (k, v1, v2) values (?, ?, ?)", "third", 3, "value3");

        flush();

        assertRows(execute("SELECT * FROM %s WHERE k = ?", "first"),
            row("first", 1, "value1")
        );

        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
            row("value2")
        );

        assertRows(execute("SELECT * FROM %s"),
            row("first",  1, "value1"),
            row("second", 2, "value2"),
            row("third",  3, "value3")
        );
    }

    @Test
    public void testTableWithOneClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v1 text, v2 text, PRIMARY KEY (k, t));");

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 1, "v11", "v21");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 2, "v12", "v22");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 3, "v13", "v23");

        flush();

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 4, "v14", "v24");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 5, "v15", "v25");

        assertRows(execute("SELECT * FROM %s"),
            row("key",  1, "v11", "v21"),
            row("key",  2, "v12", "v22"),
            row("key",  3, "v13", "v23"),
            row("key",  4, "v14", "v24"),
            row("key",  5, "v15", "v25")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
            row("key",  4, "v14", "v24"),
            row("key",  5, "v15", "v25")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
            row("key",  2, "v12", "v22"),
            row("key",  3, "v13", "v23")
        );

        // Reversed queries

        assertRows(execute("SELECT * FROM %s WHERE k = ? ORDER BY t DESC", "key"),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24"),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22"),
            row("key",  1, "v11", "v21")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t > ? ORDER BY t DESC", "key", 3),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ? ORDER BY t DESC", "key", 2, 4),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22")
        );
    }

    @Test
    public void testTableWithReverseClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v1 text, v2 text, PRIMARY KEY (k, t)) WITH CLUSTERING ORDER BY (t DESC);");

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 1, "v11", "v21");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 2, "v12", "v22");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 3, "v13", "v23");

        flush();

        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 4, "v14", "v24");
        execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", 5, "v15", "v25");

        assertRows(execute("SELECT * FROM %s"),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24"),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22"),
            row("key",  1, "v11", "v21")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? ORDER BY t ASC", "key"),
            row("key",  1, "v11", "v21"),
            row("key",  2, "v12", "v22"),
            row("key",  3, "v13", "v23"),
            row("key",  4, "v14", "v24"),
            row("key",  5, "v15", "v25")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t > ?", "key", 3),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ?", "key", 2, 4),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22")
        );

        // Reversed queries

        assertRows(execute("SELECT * FROM %s WHERE k = ? ORDER BY t DESC", "key"),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24"),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22"),
            row("key",  1, "v11", "v21")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t > ? ORDER BY t DESC", "key", 3),
            row("key",  5, "v15", "v25"),
            row("key",  4, "v14", "v24")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t >= ? AND t < ? ORDER BY t DESC", "key", 2, 4),
            row("key",  3, "v13", "v23"),
            row("key",  2, "v12", "v22")
        );
    }

    @Test
    public void testTableWithTwoClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t1 text, t2 int, v text, PRIMARY KEY (k, t1, t2));");

        execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v1", 1, "v1");
        execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v1", 2, "v2");
        execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 1, "v3");
        execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 2, "v4");
        execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", "v2", 3, "v5");
        flush();

        assertRows(execute("SELECT * FROM %s"),
            row("key",  "v1", 1, "v1"),
            row("key",  "v1", 2, "v2"),
            row("key",  "v2", 1, "v3"),
            row("key",  "v2", 2, "v4"),
            row("key",  "v2", 3, "v5")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t1 >= ?", "key", "v2"),
            row("key",  "v2", 1, "v3"),
            row("key",  "v2", 2, "v4"),
            row("key",  "v2", 3, "v5")
        );

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND t1 >= ? ORDER BY t1 DESC", "key", "v2"),
            row("key",  "v2", 3, "v5"),
            row("key",  "v2", 2, "v4"),
            row("key",  "v2", 1, "v3")
        );
    }

    @Test
    public void testTableWithLargePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t1 int, t2 int, v text, PRIMARY KEY (k, t1, t2));");

        for (int t1 = 0; t1 < 20; t1++)
            for (int t2 = 0; t2 < 10; t2++)
                execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", t1, t2, "someSemiLargeTextForValue_" + t1 + "_" + t2);

        flush();

        Object[][] expected = new Object[10][];
        for (int t2 = 0; t2 < 10; t2++)
            expected[t2] = row("key", 15, t2);

        assertRows(execute("SELECT k, t1, t2 FROM %s WHERE k=? AND t1=?", "key", 15), expected);

        Object[][] expectedReverse = new Object[10][];
        for (int t2 = 9; t2 >= 0; t2--)
            expectedReverse[9 - t2] = row("key", 15, t2);

        assertRows(execute("SELECT k, t1, t2 FROM %s WHERE k=? AND t1=? ORDER BY t1 DESC, t2 DESC", "key", 15), expectedReverse);
    }

    @Test
    public void testRowDeletion() throws Throwable
    {
        int N = 4;

        createTable("CREATE TABLE %s (k text, t int, v1 text, v2 int, PRIMARY KEY (k, t));");

        for (int t = 0; t < N; t++)
                execute("INSERT INTO %s (k, t, v1, v2) values (?, ?, ?, ?)", "key", t, "v" + t, t + 10);

        flush();

        for (int i = 0; i < N / 2; i++)
            execute("DELETE FROM %s WHERE k=? AND t=?", "key", i * 2);

        Object[][] expected = new Object[N/2][];
        for (int i = 0; i < N / 2; i++)
        {
            int t = i * 2 + 1;
            expected[i] = row("key", t, "v" + t, t + 10);
        }

        assertRows(execute("SELECT * FROM %s"), expected);
    }

    @Test
    public void testRangeTombstones() throws Throwable
    {
        int N = 100;

        createTable("CREATE TABLE %s (k text, t1 int, t2 int, v text, PRIMARY KEY (k, t1, t2));");

        for (int t1 = 0; t1 < 3; t1++)
            for (int t2 = 0; t2 < N; t2++)
                execute("INSERT INTO %s (k, t1, t2, v) values (?, ?, ?, ?)", "key", t1, t2, "someSemiLargeTextForValue_" + t1 + "_" + t2);

        flush();

        execute("DELETE FROM %s WHERE k=? AND t1=?", "key", 1);

        flush();

        Object[][] expected = new Object[2*N][];
        for (int t2 = 0; t2 < N; t2++)
        {
            expected[t2] = row("key", 0, t2, "someSemiLargeTextForValue_0_" + t2);
            expected[N + t2] = row("key", 2, t2, "someSemiLargeTextForValue_2_" + t2);
        }

        assertRows(execute("SELECT * FROM %s"), expected);
    }

    @Test
    public void test2ndaryIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v text, PRIMARY KEY (k, t));");

        execute("CREATE INDEX ON %s(v)");

        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 1, "foo");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 2, "bar");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 1, "foo");

        flush();

        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 2, "foo");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 3, "bar");

        assertRows(execute("SELECT * FROM %s WHERE v = ?", "foo"),
            row("key1",  1, "foo"),
            row("key2",  1, "foo"),
            row("key2",  2, "foo")
        );

        assertRows(execute("SELECT * FROM %s WHERE v = ?", "bar"),
            row("key1",  2, "bar"),
            row("key2",  3, "bar")
        );
    }

    @Test
    public void testStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, s text static, v text, PRIMARY KEY (k, t));");

        execute("INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 1, "foo1", "st1");
        execute("INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 2, "foo2", "st2");

        flush();

        execute("INSERT INTO %s (k, t, v, s) values (?, ?, ?, ?)", "key1", 3, "foo3", "st3");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 4, "foo4");

        assertRows(execute("SELECT * FROM %s"),
            row("key1",  1, "st3", "foo1"),
            row("key1",  2, "st3", "foo2"),
            row("key1",  3, "st3", "foo3"),
            row("key1",  4, "st3", "foo4")
        );

        assertRows(execute("SELECT s FROM %s WHERE k = ?", "key1"),
            row("st3"),
            row("st3"),
            row("st3"),
            row("st3")
        );

        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k = ?", "key1"),
            row("st3")
        );
    }

    @Test
    public void testDistinct() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v text, PRIMARY KEY (k, t));");

        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 1, "foo1");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 2, "foo2");

        flush();

        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key1", 3, "foo3");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 4, "foo4");
        execute("INSERT INTO %s (k, t, v) values (?, ?, ?)", "key2", 5, "foo5");

        assertRows(execute("SELECT DISTINCT k FROM %s"),
            row("key1"),
            row("key2")
        );
    }

    @Test
    public void collectionDeletionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, s set<int>);");

        execute("INSERT INTO %s (k, s) VALUES (?, ?)", 1, set(1));

        flush();

        execute("INSERT INTO %s (k, s) VALUES (?, ?)", 1, set(2));

        assertRows(execute("SELECT s FROM %s WHERE k = ?", 1),
            row(set(2))
        );
    }

    @Test
    public void limitWithMultigetTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int);");

        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 0, 0);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (k, v) VALUES (?, ?)", 3, 3);

        assertRows(execute("SELECT v FROM %s WHERE k IN ? LIMIT ?", list(0, 1, 2, 3), 2),
            row(0),
            row(1)
        );
    }

    @Test
    public void staticDistinctTest() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, p int, s int static, PRIMARY KEY (k, p))");

        execute("INSERT INTO %s (k, p) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (k, p) VALUES (?, ?)", 1, 2);

        assertRows(execute("SELECT k, s FROM %s"),
            row(1, null),
            row(1, null)
        );
        assertRows(execute("SELECT DISTINCT k, s FROM %s"),
            row(1, null)
        );
        assertRows(execute("SELECT DISTINCT s FROM %s WHERE k=?", 1),
            row((Object)null)
        );
        assertEmpty(execute("SELECT DISTINCT s FROM %s WHERE k=?", 2));
    }
}
