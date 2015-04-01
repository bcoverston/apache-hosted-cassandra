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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A set of single column restrictions on a primary key part (partition key or clustering key).
 */
final class PrimaryKeyRestrictionSet extends AbstractPrimaryKeyRestrictions
{
    /**
     * The restrictions.
     */
    private final RestrictionSet restrictions;

    /**
     * <code>true</code> if the restrictions are corresponding to an EQ, <code>false</code> otherwise.
     */
    private boolean eq;

    /**
     * <code>true</code> if the restrictions are corresponding to an IN, <code>false</code> otherwise.
     */
    private boolean in;

    /**
     * <code>true</code> if the restrictions are corresponding to a Slice, <code>false</code> otherwise.
     */
    private boolean slice;

    /**
     * <code>true</code> if the restrictions are corresponding to a Contains, <code>false</code> otherwise.
     */
    private boolean contains;

    /**
     * <code>true</code> if the restrictions corresponding to a partition key, <code>false</code> if it's clustering columns.
     */
    private boolean isPartitionKey;

    public PrimaryKeyRestrictionSet(ClusteringComparator comparator, boolean isPartitionKey)
    {
        super(comparator);
        this.restrictions = new RestrictionSet();
        this.eq = true;
        this.isPartitionKey = isPartitionKey;
    }

    private PrimaryKeyRestrictionSet(PrimaryKeyRestrictionSet primaryKeyRestrictions,
                                               Restriction restriction) throws InvalidRequestException
    {
        super(primaryKeyRestrictions.comparator);
        this.restrictions = primaryKeyRestrictions.restrictions.addRestriction(restriction);
        this.isPartitionKey = primaryKeyRestrictions.isPartitionKey;

        if (!primaryKeyRestrictions.isEmpty())
        {
            ColumnDefinition lastRestrictionStart = primaryKeyRestrictions.restrictions.lastRestriction().getFirstColumn();
            ColumnDefinition newRestrictionStart = restriction.getFirstColumn();

            checkFalse(primaryKeyRestrictions.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(),
                       "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                       newRestrictionStart.name,
                       lastRestrictionStart.name);

            if (newRestrictionStart.position() < lastRestrictionStart.position() && restriction.isSlice())
                throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                                     restrictions.nextColumn(newRestrictionStart).name,
                                     newRestrictionStart.name);
        }

        if (restriction.isSlice() || primaryKeyRestrictions.isSlice())
            this.slice = true;
        else if (restriction.isContains() || primaryKeyRestrictions.isContains())
            this.contains = true;
        else if (restriction.isIN())
            this.in = true;
        else
            this.eq = true;
    }

    private List<ByteBuffer> toByteBuffers(SortedSet<? extends ClusteringPrefix> clusterings)
    {
        // It's currently a tad hard to follow that this is only called for partition key so we should fix that
        List<ByteBuffer> l = new ArrayList<>(clusterings.size());
        for (ClusteringPrefix clustering : clusterings)
            l.add(CFMetaData.serializePartitionKey(clustering));
        return l;
    }

    @Override
    public boolean isSlice()
    {
        return slice;
    }

    @Override
    public boolean isEQ()
    {
        return eq;
    }

    @Override
    public boolean isIN()
    {
        return in;
    }

    @Override
    public boolean isOnToken()
    {
        return false;
    }

    @Override
    public boolean isContains()
    {
        return contains;
    }

    @Override
    public boolean isMultiColumn()
    {
        return false;
    }

    @Override
    public boolean usesFunction(String ksName, String functionName)
    {
        return restrictions.usesFunction(ksName, functionName);
    }

    @Override
    public PrimaryKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        if (restriction.isOnToken())
        {
            if (isEmpty())
                return (PrimaryKeyRestrictions) restriction;

            return new TokenFilter(this, (TokenRestriction) restriction);
        }

        return new PrimaryKeyRestrictionSet(this, restriction);
    }

    @Override
    public SortedSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        return appendTo(MultiCBuilder.create(comparator), options).build();
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        for (Restriction r : restrictions)
        {
            r.appendTo(builder, options);
            if (builder.hasMissingElements())
                break;
        }
        return builder;
    }

    @Override
    public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator);
        int keyPosition = 0;
        for (Restriction r : restrictions)
        {
            ColumnDefinition def = r.getFirstColumn();

            // The bound of this method is refering to the clustering order. So if said clustering order
            // is reversed for this column, we should reverse the restriction we use.
            Bound b = !def.isReversedType() ? bound : bound.reverse();
            if (keyPosition != def.position() || r.isContains())
                break;

            if (r.isSlice())
            {
                if (!r.hasBound(b))
                {
                    // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                    // For composites, if there was preceding component and we're computing the end, we must change the last component
                    // End-Of-Component, otherwise we would be selecting only one record.
                    return builder.buildBound(bound.isStart(), true);
                }

                r.appendBoundTo(builder, b, options);
                return builder.buildBound(bound.isStart(), r.isInclusive(b));
            }

            r.appendBoundTo(builder, b, options);

            if (builder.hasMissingElements())
                return FBUtilities.<Slice.Bound>emptySortedSet(comparator);

            keyPosition = r.getLastColumn().position() + 1;
        }

        // Everything was an equal (or there was nothing)
        return builder.buildBound(bound.isStart(), true);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
    {
        if (!isPartitionKey)
            throw new UnsupportedOperationException();

        return toByteBuffers(valuesAsClustering(options));
    }

    @Override
    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        if (!isPartitionKey)
            throw new UnsupportedOperationException();

        return toByteBuffers(boundsAsClustering(b, options));
    }

    @Override
    public boolean hasBound(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().hasBound(b);
    }

    @Override
    public boolean isInclusive(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().isInclusive(b);
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        return restrictions.hasSupportingIndex(indexManager);
    }

    @Override
    public void addColumnFilterTo(ColumnFilter filter,
                                  SecondaryIndexManager indexManager,
                                  QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        for (Restriction restriction : restrictions)
        {
            ColumnDefinition columnDef = restriction.getFirstColumn();

            // We ignore all the clustering columns that can be handled by slices.
            if (!isPartitionKey && !restriction.isContains()&& position == columnDef.position())
            {
                position = restriction.getLastColumn().position() + 1;
                if (!restriction.hasSupportingIndex(indexManager))
                    continue;
            }
            restriction.addColumnFilterTo(filter, indexManager, options);
        }
    }

    @Override
    public Collection<ColumnDefinition> getColumnDefs()
    {
        return restrictions.getColumnDefs();
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return restrictions.firstColumn();
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return restrictions.lastColumn();
    }
}
