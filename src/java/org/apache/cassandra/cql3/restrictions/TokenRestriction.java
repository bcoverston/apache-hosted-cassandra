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

import com.google.common.base.Joiner;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * <code>Restriction</code> using the token function.
 */
public abstract class TokenRestriction extends AbstractPrimaryKeyRestrictions
{
    /**
     * The definition of the columns to which apply the token restriction.
     */
    protected final List<ColumnDefinition> columnDefs;

    /**
     * Creates a new <code>TokenRestriction</code> that apply to the specified columns.
     *
     * @param comparator the clustering comparator
     * @param columnDefs the definition of the columns to which apply the token restriction
     */
    public TokenRestriction(ClusteringComparator comparator, List<ColumnDefinition> columnDefs)
    {
        super(comparator);
        this.columnDefs = columnDefs;
    }

    @Override
    public  boolean isOnToken()
    {
        return true;
    }

    @Override
    public Collection<ColumnDefinition> getColumnDefs()
    {
        return columnDefs;
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return columnDefs.get(0);
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return columnDefs.get(columnDefs.size() - 1);
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager secondaryIndexManager)
    {
        return false;
    }

    @Override
    public void addColumnFilterTo(ColumnFilter filter, SecondaryIndexManager indexManager, QueryOptions options)
    {
        throw new UnsupportedOperationException("Index expression cannot be created for token restriction");
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the column names as a comma separated <code>String</code>.
     *
     * @return the column names as a comma separated <code>String</code>.
     */
    protected final String getColumnNamesAsString()
    {
        return Joiner.on(", ").join(ColumnDefinition.toIdentifiers(columnDefs));
    }

    @Override
    public final PrimaryKeyRestrictions mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
        if (!otherRestriction.isOnToken())
            return new TokenFilter(toPrimaryKeyRestriction(otherRestriction), this);

        return doMergeWith((TokenRestriction) otherRestriction);
    }

    /**
     * Merges this restriction with the specified <code>TokenRestriction</code>.
     * @param otherRestriction the <code>TokenRestriction</code> to merge with.
     */
    protected abstract PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException;

    /**
     * Converts the specified restriction into a <code>PrimaryKeyRestrictions</code>.
     *
     * @param restriction the restriction to convert
     * @return a <code>PrimaryKeyRestrictions</code>
     * @throws InvalidRequestException if a problem occurs while converting the restriction
     */
    private PrimaryKeyRestrictions toPrimaryKeyRestriction(Restriction restriction) throws InvalidRequestException
    {
        if (restriction instanceof PrimaryKeyRestrictions)
            return (PrimaryKeyRestrictions) restriction;

        return new PrimaryKeyRestrictionSet(comparator, true).mergeWith(restriction);
    }

    public static final class EQRestriction extends TokenRestriction
    {
        private final Term value;

        public EQRestriction(ClusteringComparator comparator, List<ColumnDefinition> columnDefs, Term value)
        {
            super(comparator, columnDefs);
            this.value = value;
        }

        @Override
        public boolean isEQ()
        {
            return true;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(value, ksName, functionName);
        }

        @Override
        protected PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 Joiner.on(", ").join(ColumnDefinition.toIdentifiers(columnDefs)));
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(value.bindAndGet(options));
        }
    }

    public static class SliceRestriction extends TokenRestriction
    {
        private final TermSlice slice;

        public SliceRestriction(ClusteringComparator comparator, List<ColumnDefinition> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            super(comparator, columnDefs);
            slice = TermSlice.newInstance(bound, inclusive, term);
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(slice.bound(b).bindAndGet(options));
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return (slice.hasBound(Bound.START) && usesFunction(slice.bound(Bound.START), ksName, functionName))
                    || (slice.hasBound(Bound.END) && usesFunction(slice.bound(Bound.END), ksName, functionName));
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        protected PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction)
        throws InvalidRequestException
        {
            if (!otherRestriction.isSlice())
                throw invalidRequest("Columns \"%s\" cannot be restricted by both an equality and an inequality relation",
                                     getColumnNamesAsString());

            TokenRestriction.SliceRestriction otherSlice = (TokenRestriction.SliceRestriction) otherRestriction;

            if (hasBound(Bound.START) && otherSlice.hasBound(Bound.START))
                throw invalidRequest("More than one restriction was found for the start bound on %s",
                                     getColumnNamesAsString());

            if (hasBound(Bound.END) && otherSlice.hasBound(Bound.END))
                throw invalidRequest("More than one restriction was found for the end bound on %s",
                                     getColumnNamesAsString());

            return new SliceRestriction(comparator, columnDefs,  slice.merge(otherSlice.slice));
        }

        @Override
        public String toString()
        {
            return String.format("SLICE%s", slice);
        }
        private SliceRestriction(ClusteringComparator comparator, List<ColumnDefinition> columnDefs, TermSlice slice)
        {
            super(comparator, columnDefs);
            this.slice = slice;
        }
    }
}
