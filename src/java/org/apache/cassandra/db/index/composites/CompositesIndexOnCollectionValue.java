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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;

/**
 * Index the value of a collection cell.
 *
 * This is a lot like an index on REGULAR, except that we also need to make
 * the collection key part of the index entry so that:
 *   1) we don't have to scan the whole collection at query time to know the
 *   entry is stale and if it still satisfies the query.
 *   2) if a collection has multiple time the same value, we need one entry
 *   for each so that if we delete one of the value only we only delete the
 *   entry corresponding to that value.
 */
public class CompositesIndexOnCollectionValue extends CompositesIndex
{
    public static ClusteringComparator buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<>(prefixSize + 2);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(baseMetadata.comparator.subtype(i));
        types.add(((CollectionType)columnDef.type).nameComparator()); // collection key
        return new ClusteringComparator(types, true, true);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).valueComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path)
    {
        return cellValue;
    }

    protected CBuilder buildIndexClusteringPrefix(ByteBuffer rowKey, ClusteringPrefix prefix, Cell cell)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), prefix.size()); i++)
            builder.add(prefix.get(i));

        // When indexing, cell will be present, but when searching, it won't  (CASSANDRA-7525)
        if (prefix.size() == baseCfs.metadata.clusteringColumns().size() && cell != null)
            builder.add(cell.path().get(0));

        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        int prefixSize = columnDef.position();
        Clustering clustering = indexEntry.clustering();
        CBuilder builder = CBuilder.create(baseCfs.getComparator());
        for (int i = 0; i < prefixSize; i++)
            builder.add(clustering.get(i + 1));
        return new IndexedEntry(indexedValue, clustering, indexEntry.partitionKeyLivenessInfo().timestamp(), clustering.get(0), builder.build());
    }

    @Override
    public boolean supportsOperator(Operator operator)
    {
        return operator == Operator.CONTAINS && !(columnDef.type instanceof SetType);
    }

    public boolean isStale(Row data, ByteBuffer indexValue)
    {
        Iterator<Cell> iter = data.getCells(columnDef);
        while (iter.hasNext())
        {
            Cell cell = iter.next();
            if (cell.isLive(data.nowInSec()) && ((CollectionType) columnDef.type).valueComparator().compare(indexValue, cell.value()) == 0)
                return false;
        }
        return true;
    }
}
