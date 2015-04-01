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
package org.apache.cassandra.db;

import java.util.*;
import java.security.MessageDigest;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * Columns (or a subset of the columns) that a partition contains.
 * This mainly groups both static and regular columns for convenience.
 */
public class PartitionColumns implements Iterable<ColumnDefinition>
{
    public static PartitionColumns NONE = new PartitionColumns(Columns.NONE, Columns.NONE);

    public final Columns statics;
    public final Columns regulars;

    public PartitionColumns(Columns statics, Columns regulars)
    {
        this.statics = statics;
        this.regulars = regulars;
    }

    public static PartitionColumns of(ColumnDefinition column)
    {
        return new PartitionColumns(column.isStatic() ? Columns.of(column) : Columns.NONE,
                                    column.isStatic() ? Columns.NONE : Columns.of(column));
    }

    public PartitionColumns without(ColumnDefinition column)
    {
        return new PartitionColumns(column.isStatic() ? statics.without(column) : statics,
                                    column.isStatic() ? regulars : regulars.without(column));
    }

    public boolean isEmpty()
    {
        return statics.isEmpty() && regulars.isEmpty();
    }

    public boolean contains(ColumnDefinition column)
    {
        return column.isStatic() ? statics.contains(column) : regulars.contains(column);
    }

    public boolean includes(PartitionColumns columns)
    {
        return statics.contains(columns.statics) && regulars.contains(columns.regulars);
    }

    public Iterator<ColumnDefinition> iterator()
    {
        return Iterators.concat(statics.iterator(), regulars.iterator());
    }

    public Iterator<ColumnDefinition> selectOrderIterator()
    {
        return Iterators.concat(statics.selectOrderIterator(), regulars.selectOrderIterator());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(statics).append(" | ").append(regulars).append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof PartitionColumns))
            return false;

        PartitionColumns that = (PartitionColumns)other;
        return this.statics.equals(that.statics)
            && this.regulars.equals(that.regulars);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statics, regulars);
    }

    public void digest(MessageDigest digest)
    {
        regulars.digest(digest);
        statics.digest(digest);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        // Note that we do want to use sorted sets because we want the column definitions to be compared
        // through compareTo, not equals. The former basically check it's the same column name, while the latter
        // check it's the same object, including the same type.
        private SortedSet<ColumnDefinition> regularColumns;
        private SortedSet<ColumnDefinition> staticColumns;

        public Builder add(ColumnDefinition c)
        {
            if (c.isStatic())
            {
                if (staticColumns == null)
                    staticColumns = new TreeSet<>();
                staticColumns.add(c);
            }
            else
            {
                if (regularColumns == null)
                    regularColumns = new TreeSet<>();
                regularColumns.add(c);
            }
            return this;
        }

        public int added()
        {
            return (regularColumns == null ? 0 : regularColumns.size())
                 + (staticColumns == null ? 0 : staticColumns.size());
        }

        public Builder addAll(Iterable<ColumnDefinition> columns)
        {
            for (ColumnDefinition c : columns)
                add(c);
            return this;
        }

        public Builder addAll(PartitionColumns columns)
        {
            if (regularColumns == null && !columns.regulars.isEmpty())
                regularColumns = new TreeSet<>();

            for (ColumnDefinition c : columns.regulars)
                regularColumns.add(c);

            if (staticColumns == null && !columns.statics.isEmpty())
                staticColumns = new TreeSet<>();

            for (ColumnDefinition c : columns.statics)
                staticColumns.add(c);

            return this;
        }

        public PartitionColumns build()
        {
            return new PartitionColumns(staticColumns == null ? Columns.NONE : Columns.from(staticColumns),
                                        regularColumns == null ? Columns.NONE : Columns.from(regularColumns));
        }
    }
}
