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
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Index on a CLUSTERING_COLUMN column definition.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is always indexed by this index (or rather, it is indexed if
 * n >= columnDef.componentIndex, which will always be the case in practice)
 * and it will generate (makeIndexColumnName()) an index entry whose:
 *   - row key will be ck_i (getIndexedValue()) where i == columnDef.componentIndex.
 *   - cell name will
 *       rk ck_0 ... ck_{i-1} ck_{i+1} ck_n
 *     where rk is the row key of the initial cell and i == columnDef.componentIndex.
 */
public class CompositesIndexOnClusteringKey extends CompositesIndex
{
    public static void addClusteringColumns(CFMetaData.Builder indexMetadata, CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        indexMetadata.addClusteringColumn("partition_key", SecondaryIndex.keyComparator);

        List<ColumnDefinition> cks = baseMetadata.clusteringColumns();
        for (int i = 0; i < columnDef.position(); i++)
        {
            ColumnDefinition def = cks.get(i);
            indexMetadata.addClusteringColumn(def.name, def.type);
        }
        for (int i = columnDef.position() + 1; i < cks.size(); i++)
        {
            ColumnDefinition def = cks.get(i);
            indexMetadata.addClusteringColumn(def.name, def.type);
        }
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path)
    {
        return clustering.get(columnDef.position());
    }

    protected CBuilder buildIndexClusteringPrefix(ByteBuffer rowKey, ClusteringPrefix prefix, Cell cell)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), prefix.size()); i++)
            builder.add(prefix.get(i));
        for (int i = columnDef.position() + 1; i < prefix.size(); i++)
            builder.add(prefix.get(i));
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringColumns().size();

        Clustering clustering = indexEntry.clustering();
        CBuilder builder = CBuilder.create(baseCfs.getComparator());
        for (int i = 0; i < columnDef.position(); i++)
            builder.add(clustering.get(i + 1));

        builder.add(indexedValue.getKey());

        for (int i = columnDef.position() + 1; i < ckCount; i++)
            builder.add(clustering.get(i));

        return new IndexedEntry(indexedValue, clustering, indexEntry.partitionKeyLivenessInfo().timestamp(), clustering.get(0), builder.build());
    }

    @Override
    public boolean indexes(ColumnDefinition c)
    {
        // Actual indexing for this index type is done through maybeIndex
        return false;
    }

    public boolean isStale(Row data, ByteBuffer indexValue)
    {
        return !data.hasLiveData();
    }

    @Override
    public void maybeIndex(ByteBuffer partitionKey, Clustering clustering, long timestamp, int ttl, OpOrder.Group opGroup, int nowInSec)
    {
        if (clustering.get(columnDef.position()) != null)
            insert(partitionKey, clustering, null, SimpleLivenessInfo.forUpdate(timestamp, ttl, nowInSec, indexCfs.metadata), opGroup, nowInSec);
    }

    @Override
    public void maybeDelete(ByteBuffer partitionKey, Clustering clustering, DeletionTime deletion, OpOrder.Group opGroup, int nowInSec)
    {
        if (clustering.get(columnDef.position()) != null && !deletion.isLive())
            delete(partitionKey, clustering, null, deletion, opGroup, nowInSec);
    }

    @Override
    public void delete(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec)
    {
        // We only know that one column of the CQL row has been updated/deleted, but we don't know if the
        // full row has been deleted so we should not do anything. If it ends up that the whole row has
        // been deleted, it will be eventually cleaned up on read because the entry will be detected stale.
    }
}
