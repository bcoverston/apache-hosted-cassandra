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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.MultiItemTerminal;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrimaryKeyRestrictionSetTest
{
    @Test
    public void testboundsAsClusteringWithNoRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(1);

        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));
    }

    /**
     * Test 'clustering_0 = 1' with only one clustering column
     */
    @Test
    public void testboundsAsClusteringWithOneEqRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(1);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleEq(cfMetaData, 0, clustering_0);

        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, clustering_0);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, clustering_0);
    }

    /**
     * Test 'clustering_1 = 1' with 2 clustering columns
     */
    @Test
    public void testboundsAsClusteringWithOneEqRestrictionsAndTwoClusteringColumns() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer clustering_0 = ByteBufferUtil.bytes(1);
        Restriction eq = newSingleEq(cfMetaData, 0, clustering_0);

        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, clustering_0);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, clustering_0);
    }

    /**
     * Test 'clustering_0 IN (1, 2, 3)' with only one clustering column
     */
    @Test
    public void testboundsAsClusteringWithOneInRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        CFMetaData cfMetaData = newCFMetaData(2);

        Restriction in = newSingleIN(cfMetaData, 0, value1, value2, value3);

        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(in);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);
        assertStartBound(get(bounds, 1), true, value2);
        assertStartBound(get(bounds, 2), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);
        assertEndBound(get(bounds, 1), true, value2);
        assertEndBound(get(bounds, 2), true, value3);
    }

    /**
     * Test slice restriction (e.g 'clustering_0 > 1') with only one clustering column
     */
    @Test
    public void testboundsAsClusteringWithSliceRestrictionsAndOneClusteringColumn() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(1);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newSingleSlice(cfMetaData, 0, Bound.START, false, value1);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(cfMetaData, 0, Bound.START, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newSingleSlice(cfMetaData, 0, Bound.END, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newSingleSlice(cfMetaData, 0, Bound.END, false, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newSingleSlice(cfMetaData, 0, Bound.START, false, value1);
        Restriction slice2 = newSingleSlice(cfMetaData, 0, Bound.END, false, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        slice = newSingleSlice(cfMetaData, 0, Bound.START, true, value1);
        slice2 = newSingleSlice(cfMetaData, 0, Bound.END, true, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2);
    }

    /**
     * Test 'clustering_0 = 1 AND clustering_1 IN (1, 2, 3)'
     */
    @Test
    public void testboundsAsClusteringWithEqAndInRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        Restriction eq = newSingleEq(cfMetaData, 0, value1);
        Restriction in = newSingleIN(cfMetaData, 1, value1, value2, value3);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(in);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value1);
        assertStartBound(get(bounds, 1), true, value1, value2);
        assertStartBound(get(bounds, 2), true, value1, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(3, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value1);
        assertEndBound(get(bounds, 1), true, value1, value2);
        assertEndBound(get(bounds, 2), true, value1, value3);
    }

    /**
     * Test equal and slice restrictions (e.g 'clustering_0 = 0 clustering_1 > 1')
     */
    @Test
    public void testboundsAsClusteringWithEqAndSliceRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        Restriction eq = newSingleEq(cfMetaData, 0, value3);

        Restriction slice = newSingleSlice(cfMetaData, 1, Bound.START, false, value1);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3);

        slice = newSingleSlice(cfMetaData, 1, Bound.START, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3);

        slice = newSingleSlice(cfMetaData, 1, Bound.END, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3, value1);

        slice = newSingleSlice(cfMetaData, 1, Bound.END, false, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value3, value1);

        slice = newSingleSlice(cfMetaData, 1, Bound.START, false, value1);
        Restriction slice2 = newSingleSlice(cfMetaData, 1, Bound.END, false, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value3, value2);

        slice = newSingleSlice(cfMetaData, 1, Bound.START, true, value1);
        slice2 = newSingleSlice(cfMetaData, 1, Bound.END, true, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq).mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value3, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value3, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) = (1, 2)' with two clustering column
     */
    @Test
    public void testboundsAsClusteringWithMultiEqRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        Restriction eq = newMultiEq(cfMetaData, 0, value1, value2);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(eq);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
    }

    /**
     * Test '(clustering_0, clustering_1) IN ((1, 2), (2, 3))' with two clustering column
     */
    @Test
    public void testboundsAsClusteringWithMultiInRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        Restriction in = newMultiIN(cfMetaData, 0, asList(value1, value2), asList(value2, value3));
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(in);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);
        assertStartBound(get(bounds, 1), true, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
        assertEndBound(get(bounds, 1), true, value2, value3);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0) > (1)') with only one clustering column
     */
    @Test
    public void testboundsAsClusteringWithMultiSliceRestrictionsWithOneClusteringColumn() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(1);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        Restriction slice = newMultiSlice(cfMetaData, 0, Bound.START, false, value1);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(cfMetaData, 0, Bound.START, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        slice = newMultiSlice(cfMetaData, 0, Bound.END, true, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        slice = newMultiSlice(cfMetaData, 0, Bound.END, false, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1);

        slice = newMultiSlice(cfMetaData, 0, Bound.START, false, value1);
        Restriction slice2 = newMultiSlice(cfMetaData, 0, Bound.END, false, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        slice = newMultiSlice(cfMetaData, 0, Bound.START, true, value1);
        slice2 = newMultiSlice(cfMetaData, 0, Bound.END, true, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2);
    }

    /**
     * Test multi-column slice restrictions (e.g '(clustering_0, clustering_1) > (1, 2)')
     */
    @Test
    public void testboundsAsClusteringWithMultiSliceRestrictionsWithTwoClusteringColumn() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(2);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);

        // (clustering_0, clustering1) > (1, 2)
        Restriction slice = newMultiSlice(cfMetaData, 0, Bound.START, false, value1, value2);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1) >= (1, 2)
        slice = newMultiSlice(cfMetaData, 0, Bound.START, true, value1, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyEnd(get(bounds, 0));

        // (clustering_0, clustering1) <= (1, 2)
        slice = newMultiSlice(cfMetaData, 0, Bound.END, true, value1, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);

        // (clustering_0, clustering1) < (1, 2)
        slice = newMultiSlice(cfMetaData, 0, Bound.END, false, value1, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEmptyStart(get(bounds, 0));

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value2);

        // (clustering_0, clustering1) > (1, 2) AND (clustering_0) < (2)
        slice = newMultiSlice(cfMetaData, 0, Bound.START, false, value1, value2);
        Restriction slice2 = newMultiSlice(cfMetaData, 0, Bound.END, false, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value2);

        // (clustering_0, clustering1) >= (1, 2) AND (clustering_0, clustering1) <= (2, 1)
        slice = newMultiSlice(cfMetaData, 0, Bound.START, true, value1, value2);
        slice2 = newMultiSlice(cfMetaData, 0, Bound.END, true, value2, value1);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(slice).mergeWith(slice2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value2, value1);
    }

    /**
     * Test mixing single and multi equals restrictions (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3))
     */
    @Test
    public void testboundsAsClusteringWithSingleEqAndMultiEqRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(4);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3)
        Restriction singleEq = newSingleEq(cfMetaData, 0, value1);
        Restriction multiEq = newMultiEq(cfMetaData, 1, value2, value3);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiEq);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 2 AND (clustering_2, clustering_3) = (3, 4)
        singleEq = newSingleEq(cfMetaData, 0, value1);
        Restriction singleEq2 = newSingleEq(cfMetaData, 1, value2);
        multiEq = newMultiEq(cfMetaData, 2, value3, value4);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(singleEq2).mergeWith(multiEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 = 3
        singleEq = newSingleEq(cfMetaData, 2, value3);
        multiEq = newMultiEq(cfMetaData, 0, value1, value2);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND (clustering_1, clustering_2) = (2, 3) AND clustering_3 = 4
        singleEq = newSingleEq(cfMetaData, 0, value1);
        singleEq2 = newSingleEq(cfMetaData, 3, value4);
        multiEq = newMultiEq(cfMetaData, 1, value2, value3);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiEq).mergeWith(singleEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
    }

    /**
     * Test clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
     */
    @Test
    public void testboundsAsClusteringWithSingleEqAndMultiINRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(4);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3), (4, 5))
        Restriction singleEq = newSingleEq(cfMetaData, 0, value1);
        Restriction multiIN = newMultiIN(cfMetaData, 1, asList(value2, value3), asList(value4, value5));
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiIN);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);
        assertStartBound(get(bounds, 1), true, value1, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);
        assertEndBound(get(bounds, 1), true, value1, value4, value5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) IN ((2, 3))
        singleEq = newSingleEq(cfMetaData, 0, value1);
        multiIN = newMultiIN(cfMetaData, 1, asList(value2, value3));
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiIN).mergeWith(singleEq);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3);

        // clustering_0 = 1 AND clustering_1 = 5 AND (clustering_2, clustering_3) IN ((2, 3), (4, 5))
        singleEq = newSingleEq(cfMetaData, 0, value1);
        Restriction singleEq2 = newSingleEq(cfMetaData, 1, value5);
        multiIN = newMultiIN(cfMetaData, 2, asList(value2, value3), asList(value4, value5));
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiIN).mergeWith(singleEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value5, value2, value3);
        assertStartBound(get(bounds, 1), true, value1, value5, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value5, value2, value3);
        assertEndBound(get(bounds, 1), true, value1, value5, value4, value5);
    }

    /**
     * Test mixing single equal restrictions with multi-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testboundsAsClusteringWithSingleEqAndSliceRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(3);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3)
        Restriction singleEq = newSingleEq(cfMetaData, 0, value1);
        Restriction multiSlice = newMultiSlice(cfMetaData, 1, Bound.START, false, value2, value3);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(singleEq).mergeWith(multiSlice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1);

        // clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3) AND (clustering_1) < (4)
        singleEq = newSingleEq(cfMetaData, 0, value1);
        multiSlice = newMultiSlice(cfMetaData, 1, Bound.START, false, value2, value3);
        Restriction multiSlice2 = newMultiSlice(cfMetaData, 1, Bound.END, false, value4);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiSlice2).mergeWith(singleEq).mergeWith(multiSlice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), false, value1, value4);

        // clustering_0 = 1 AND (clustering_1, clustering_2) => (2, 3) AND (clustering_1, clustering_2) <= (4, 5)
        singleEq = newSingleEq(cfMetaData, 0, value1);
        multiSlice = newMultiSlice(cfMetaData, 1, Bound.START, true, value2, value3);
        multiSlice2 = newMultiSlice(cfMetaData, 1, Bound.END, true, value4, value5);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiSlice2).mergeWith(singleEq).mergeWith(multiSlice);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value4, value5);
    }

    /**
     * Test mixing multi equal restrictions with single-column slice restrictions
     * (e.g. clustering_0 = 1 AND (clustering_1, clustering_2) > (2, 3))
     */
    @Test
    public void testboundsAsClusteringWithMultiEqAndSingleSliceRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(3);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);

        // (clustering_0, clustering_1) = (1, 2) AND clustering_2 > 3
        Restriction multiEq = newMultiEq(cfMetaData, 0, value1, value2);
        Restriction singleSlice = newSingleSlice(cfMetaData, 2, Bound.START, false, value3);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiEq).mergeWith(singleSlice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);
    }

    @Test
    public void testboundsAsClusteringWithSeveralMultiColumnRestrictions() throws InvalidRequestException
    {
        CFMetaData cfMetaData = newCFMetaData(4);

        ByteBuffer value1 = ByteBufferUtil.bytes(1);
        ByteBuffer value2 = ByteBufferUtil.bytes(2);
        ByteBuffer value3 = ByteBufferUtil.bytes(3);
        ByteBuffer value4 = ByteBufferUtil.bytes(4);
        ByteBuffer value5 = ByteBufferUtil.bytes(5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) > (3, 4)
        Restriction multiEq = newMultiEq(cfMetaData, 0, value1, value2);
        Restriction multiSlice = newMultiSlice(cfMetaData, 2, Bound.START, false, value3, value4);
        PrimaryKeyRestrictions restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiEq).mergeWith(multiSlice);

        SortedSet<Slice.Bound> bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), false, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) IN ((3, 4), (4, 5))
        multiEq = newMultiEq(cfMetaData, 0, value1, value2);
        Restriction multiIN = newMultiIN(cfMetaData, 2, asList(value3, value4), asList(value4, value5));
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiEq).mergeWith(multiIN);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertStartBound(get(bounds, 1), true, value1, value2, value4, value5);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(2, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
        assertEndBound(get(bounds, 1), true, value1, value2, value4, value5);

        // (clustering_0, clustering_1) = (1, 2) AND (clustering_2, clustering_3) = (3, 4)
        multiEq = newMultiEq(cfMetaData, 0, value1, value2);
        Restriction multiEq2 = newMultiEq(cfMetaData, 2, value3, value4);
        restrictions = new PrimaryKeyRestrictionSet(cfMetaData.comparator, false);
        restrictions = restrictions.mergeWith(multiEq).mergeWith(multiEq2);

        bounds = restrictions.boundsAsClustering(Bound.START, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertStartBound(get(bounds, 0), true, value1, value2, value3, value4);

        bounds = restrictions.boundsAsClustering(Bound.END, QueryOptions.DEFAULT);
        assertEquals(1, bounds.size());
        assertEndBound(get(bounds, 0), true, value1, value2, value3, value4);
    }

    /**
     * Asserts that the specified <code>Bound</code> is an empty start.
     *
     * @param bound the bound to check
     */
    private static void assertEmptyStart(Slice.Bound bound)
    {
        assertEquals(Slice.Bound.BOTTOM, bound);
    }

    /**
     * Asserts that the specified <code>Bound</code> is an empty end.
     *
     * @param bound the bound to check
     */
    private static void assertEmptyEnd(Slice.Bound bound)
    {
        assertEquals(Slice.Bound.TOP, bound);
    }

    /**
     * Asserts that the specified <code>Slice.Bound</code> is a start with the specified elements.
     *
     * @param bound the bound to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertStartBound(Slice.Bound bound, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(bound, true, isInclusive, elements);
    }

    /**
     * Asserts that the specified <code>Slice.Bound</code> is a end with the specified elements.
     *
     * @param bound the bound to check
     * @param isInclusive if the bound is expected to be inclusive
     * @param elements the expected elements of the clustering
     */
    private static void assertEndBound(Slice.Bound bound, boolean isInclusive, ByteBuffer... elements)
    {
        assertBound(bound, false, isInclusive, elements);
    }

    private static void assertBound(Slice.Bound bound, boolean isStart, boolean isInclusive, ByteBuffer... elements)
    {
        assertEquals("the bound size is not the expected one:", elements.length, bound.size());
        assertEquals("the bound should be a " + (isStart ? "start" : "end") + " but is a " + (bound.isStart() ? "start" : "end"), isStart, bound.isStart());
        assertEquals("the bound inclusiveness is not the expected one", isInclusive, bound.isInclusive());
        for (int i = 0, m = elements.length; i < m; i++)
        {
            ByteBuffer element = elements[i];
            assertTrue(String.format("the element %s of the bound is not the expected one: expected %s but was %s",
                                     i,
                                     ByteBufferUtil.toInt(element),
                                     ByteBufferUtil.toInt(bound.get(i))),
                       element.equals(bound.get(i)));
        }
    }

    /**
     * Creates a new <code>CFMetaData</code> instance.
     *
     * @param numberOfClusteringColumns the number of clustering column
     * @return a new <code>CFMetaData</code> instance
     */
    private static CFMetaData newCFMetaData(int numberOfClusteringColumns)
    {
        List<AbstractType<?>> types = new ArrayList<>();

        for (int i = 0; i < numberOfClusteringColumns; i++)
            types.add(Int32Type.instance);

        ClusteringComparator comparator = new ClusteringComparator(types, false, true);
        CFMetaData cfMetaData = new CFMetaData("keyspace", "test", ColumnFamilyType.Standard, comparator);

        cfMetaData.addColumnDefinition(ColumnDefinition.partitionKeyDef("keyspace", "test", "partition_key", Int32Type.instance, 0));

        for (int i = 0; i < numberOfClusteringColumns; i++)
        {
            ColumnDefinition columnDef = ColumnDefinition.clusteringKeyDef("keyspace", "test", "clustering_" + i, Int32Type.instance, i);
            cfMetaData.addColumnDefinition(columnDef);
        }
        cfMetaData.rebuild();
        return cfMetaData;
    }

    /**
     * Creates a new <code>SingleColumnRestriction.EQ</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @param value the equality value
     * @return a new <code>SingleColumnRestriction.EQ</code> instance for the specified clustering column
     */
    private static Restriction newSingleEq(CFMetaData cfMetaData, int index, ByteBuffer value)
    {
        ColumnDefinition columnDef = getClusteringColumnDefinition(cfMetaData, index);
        return new SingleColumnRestriction.EQ(columnDef, toTerm(value));
    }

    /**
     * Creates a new <code>MultiColumnRestriction.EQ</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @param value the equality value
     * @return a new <code>MultiColumnRestriction.EQ</code> instance for the specified clustering column
     */
    private static Restriction newMultiEq(CFMetaData cfMetaData, int firstIndex, ByteBuffer... values)
    {
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnDefinitions.add(getClusteringColumnDefinition(cfMetaData, firstIndex + i));
        }
        return new MultiColumnRestriction.EQ(columnDefinitions, toMultiItemTerminal(values));
    }

    /**
     * Creates a new <code>MultiColumnRestriction.IN</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param firstIndex the index of the first clustering column
     * @param values the in values
     * @return a new <code>MultiColumnRestriction.IN</code> instance for the specified clustering column
     */
    @SafeVarargs
    private static Restriction newMultiIN(CFMetaData cfMetaData, int firstIndex, List<ByteBuffer>... values)
    {
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        List<Term> terms = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnDefinitions.add(getClusteringColumnDefinition(cfMetaData, firstIndex + i));
            terms.add(toMultiItemTerminal(values[i].toArray(new ByteBuffer[0])));
        }
        return new MultiColumnRestriction.InWithValues(columnDefinitions, terms);
    }

    /**
     * Creates a new <code>SingleColumnRestriction.IN</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @param values the in values
     * @return a new <code>SingleColumnRestriction.IN</code> instance for the specified clustering column
     */
    private static Restriction newSingleIN(CFMetaData cfMetaData, int index, ByteBuffer... values)
    {
        ColumnDefinition columnDef = getClusteringColumnDefinition(cfMetaData, index);
        return new SingleColumnRestriction.InWithValues(columnDef, toTerms(values));
    }

    /**
     * Returns the clustering <code>ColumnDefinition</code> for the specified position.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @return the clustering <code>ColumnDefinition</code> for the specified position.
     */
    private static ColumnDefinition getClusteringColumnDefinition(CFMetaData cfMetaData, int index)
    {
        return cfMetaData.clusteringColumns().get(index);
    }

    /**
     * Creates a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is inclusive
     * @param value the bound value
     * @return a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column
     */
    private static Restriction newSingleSlice(CFMetaData cfMetaData, int index, Bound bound, boolean inclusive, ByteBuffer value)
    {
        ColumnDefinition columnDef = getClusteringColumnDefinition(cfMetaData, index);
        return new SingleColumnRestriction.SliceRestriction(columnDef, bound, inclusive, toTerm(value));
    }

    /**
     * Creates a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column.
     *
     * @param cfMetaData the column family meta data
     * @param index the clustering column index
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is inclusive
     * @param value the bound value
     * @return a new <code>SingleColumnRestriction.Slice</code> instance for the specified clustering column
     */
    private static Restriction newMultiSlice(CFMetaData cfMetaData, int firstIndex, Bound bound, boolean inclusive, ByteBuffer... values)
    {
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for (int i = 0; i < values.length; i++)
        {
            columnDefinitions.add(getClusteringColumnDefinition(cfMetaData, i + firstIndex));
        }
        return new MultiColumnRestriction.SliceRestriction(columnDefinitions, bound, inclusive, toMultiItemTerminal(values));
    }

    /**
     * Converts the specified values into a <code>MultiItemTerminal</code>.
     *
     * @param values the values to convert.
     * @return the term corresponding to the specified values.
     */
    private static MultiItemTerminal toMultiItemTerminal(ByteBuffer... values)
    {
        return new Tuples.Value(values);
    }

    /**
     * Converts the specified value into a term.
     *
     * @param value the value to convert.
     * @return the term corresponding to the specified value.
     */
    private static Term toTerm(ByteBuffer value)
    {
        return new Constants.Value(value);
    }

    /**
     * Converts the specified values into a <code>List</code> of terms.
     *
     * @param values the values to convert.
     * @return a <code>List</code> of terms corresponding to the specified values.
     */
    private static List<Term> toTerms(ByteBuffer... values)
    {
        List<Term> terms = new ArrayList<>();
        for (ByteBuffer value : values)
            terms.add(toTerm(value));
        return terms;
    }

    private static <T> T get(SortedSet<T> set, int i)
    {
        return Iterables.get(set, i);
    }
}
