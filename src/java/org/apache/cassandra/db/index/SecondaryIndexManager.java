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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;

import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Manages all the indexes associated with a given CFS
 * Different types of indexes can be created across the same CF
 */
public class SecondaryIndexManager
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    public static final Updater nullUpdater = new Updater()
    {
        public void maybeIndex(Clustering clustering, long maxLiveTimestamp, int ttl, DeletionTime deletion) {}
        public void insert(Clustering clustering, Cell cell) {}
        public void update(Clustering clustering, Cell oldCell, Cell cell) {}
        public void remove(Clustering clustering, Cell current) {}

        public void updateRowLevelIndexes() {}
    };

    /**
     * Organizes the indexes by column name
     */
    private final ConcurrentNavigableMap<ByteBuffer, SecondaryIndex> indexesByColumn;


    /**
     * Keeps a single instance of a SecondaryIndex for many columns when the index type
     * has isRowLevelIndex() == true
     *
     * This allows updates to happen to an entire row at once
     */
    private final ConcurrentMap<Class<? extends SecondaryIndex>, SecondaryIndex> rowLevelIndexMap;


    /**
     * Keeps all secondary index instances, either per-column or per-row
     */
    private final Set<SecondaryIndex> allIndexes;


    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        indexesByColumn = new ConcurrentSkipListMap<>();
        rowLevelIndexMap = new ConcurrentHashMap<>();
        allIndexes = Collections.newSetFromMap(new ConcurrentHashMap<SecondaryIndex, Boolean>());

        this.baseCfs = baseCfs;
    }

    /**
     * Drops and adds new indexes associated with the underlying CF
     */
    public void reload()
    {
        // figure out what needs to be added and dropped.
        // future: if/when we have modifiable settings for secondary indexes,
        // they'll need to be handled here.
        Collection<ByteBuffer> indexedColumnNames = indexesByColumn.keySet();
        for (ByteBuffer indexedColumn : indexedColumnNames)
        {
            ColumnDefinition def = baseCfs.metadata.getColumnDefinition(indexedColumn);
            if (def == null || def.getIndexType() == null)
                removeIndexedColumn(indexedColumn);
        }

        // TODO: allow all ColumnDefinition type
        for (ColumnDefinition cdef : baseCfs.metadata.allColumns())
            if (cdef.getIndexType() != null && !indexedColumnNames.contains(cdef.name.bytes))
                addIndexedColumn(cdef);

        for (SecondaryIndex index : allIndexes)
            index.reload();
    }

    public Set<String> allIndexesNames()
    {
        Set<String> names = new HashSet<>(allIndexes.size());
        for (SecondaryIndex index : allIndexes)
            names.add(index.getIndexName());
        return names;
    }

    public Set<PerColumnSecondaryIndex> perColumnIndexes()
    {
        Set<PerColumnSecondaryIndex> s = new HashSet<>();
        for (SecondaryIndex index : allIndexes)
            if (index instanceof PerColumnSecondaryIndex)
                s.add((PerColumnSecondaryIndex)index);
        return s;
    }

    public Set<PerRowSecondaryIndex> perRowIndexes()
    {
        Set<PerRowSecondaryIndex> s = new HashSet<>();
        for (SecondaryIndex index : allIndexes)
            if (index instanceof PerRowSecondaryIndex)
                s.add((PerRowSecondaryIndex)index);
        return s;
    }

    /**
     * Does a full, blocking rebuild of the indexes specified by columns from the sstables.
     * Does nothing if columns is empty.
     *
     * Caller must acquire and release references to the sstables used here.
     *
     * @param sstables the data to build from
     * @param idxNames the list of columns to index, ordered by comparator
     */
    public void maybeBuildSecondaryIndexes(Collection<SSTableReader> sstables, Set<String> idxNames)
    {
        if (idxNames.isEmpty())
            return;

        logger.info(String.format("Submitting index build of %s for data in %s",
                                  idxNames, StringUtils.join(sstables, ", ")));

        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs, idxNames, new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        FBUtilities.waitOnFuture(future);

        flushIndexesBlocking();

        logger.info("Index build of {} complete", idxNames);
    }

    private boolean indexes(ColumnDefinition column)
    {
        for (SecondaryIndex index : allIndexes)
            if (index.indexes(column))
                return true;
        return false;
    }

    private Set<SecondaryIndex> indexFor(ColumnDefinition column)
    {
        Set<SecondaryIndex> matching = null;
        for (SecondaryIndex index : allIndexes)
        {
            if (index.indexes(column))
            {
                if (matching == null)
                    matching = new HashSet<>();
                matching.add(index);
            }
        }
        return matching == null ? Collections.<SecondaryIndex>emptySet() : matching;
    }

    /**
     * Removes a existing index
     * @param column the indexed column to remove
     */
    public void removeIndexedColumn(ByteBuffer column)
    {
        SecondaryIndex index = indexesByColumn.remove(column);

        if (index == null)
            return;

        // Remove this column from from row level index map as well as all indexes set
        if (index instanceof PerRowSecondaryIndex)
        {
            index.removeColumnDef(column);

            // If no columns left remove from row level lookup as well as all indexes set
            if (index.getColumnDefs().isEmpty())
            {
                allIndexes.remove(index);
                rowLevelIndexMap.remove(index.getClass());
            }
        }
        else
        {
            allIndexes.remove(index);
        }

        index.removeIndex(column);
        SystemKeyspace.setIndexRemoved(baseCfs.metadata.ksName, index.getNameForSystemKeyspace(column));
    }

    /**
     * Adds and builds a index for a column
     * @param cdef the column definition holding the index data
     * @return a future which the caller can optionally block on signaling the index is built
     */
    public synchronized Future<?> addIndexedColumn(ColumnDefinition cdef)
    {
        if (indexesByColumn.containsKey(cdef.name.bytes))
            return null;

        assert cdef.getIndexType() != null;

        SecondaryIndex index = SecondaryIndex.createInstance(baseCfs, cdef);

        // Keep a single instance of the index per-cf for row level indexes
        // since we want all columns to be under the index
        if (index instanceof PerRowSecondaryIndex)
        {
            SecondaryIndex currentIndex = rowLevelIndexMap.get(index.getClass());

            if (currentIndex == null)
            {
                rowLevelIndexMap.put(index.getClass(), index);
                index.init();
            }
            else
            {
                index = currentIndex;
                index.addColumnDef(cdef);
                logger.info("Creating new index : {}",cdef);
            }
        }
        else
        {
            // TODO: We sould do better than throw a RuntimeException
            if (cdef.getIndexType() == IndexType.CUSTOM && index instanceof AbstractSimplePerColumnSecondaryIndex)
                throw new RuntimeException("Cannot use a subclass of AbstractSimplePerColumnSecondaryIndex as a CUSTOM index, as they assume they are CFS backed");
            index.init();
        }

        // link in indexedColumns. this means that writes will add new data to
        // the index immediately,
        // so we don't have to lock everything while we do the build. it's up to
        // the operator to wait
        // until the index is actually built before using in queries.
        indexesByColumn.put(cdef.name.bytes, index);

        // Add to all indexes set:
        allIndexes.add(index);

        // if we're just linking in the index to indexedColumns on an
        // already-built index post-restart, we're done
        if (index.isIndexBuilt(cdef.name.bytes))
            return null;

        return index.buildIndexAsync();
    }

    /**
     *
     * @param column the name of indexes column
     * @return the index
     */
    public SecondaryIndex getIndexForColumn(ColumnDefinition column)
    {
        return indexesByColumn.get(column.name.bytes);
    }

    /**
     * Remove the index
     */
    public void invalidate()
    {
        for (SecondaryIndex index : allIndexes)
            index.invalidate();
    }

    /**
     * Flush all indexes to disk
     */
    public void flushIndexesBlocking()
    {
        // despatch flushes for all CFS backed indexes
        List<Future<?>> wait = new ArrayList<>();
        synchronized (baseCfs.getDataTracker())
        {
            for (SecondaryIndex index : allIndexes)
                if (index.getIndexCfs() != null)
                    wait.add(index.getIndexCfs().forceFlush());
        }

        // blockingFlush any non-CFS-backed indexes
        for (SecondaryIndex index : allIndexes)
            if (index.getIndexCfs() == null)
                index.forceBlockingFlush();

        // wait for the CFS-backed index flushes to complete
        FBUtilities.waitOnFutures(wait);
    }

    /**
     * @return all built indexes (ready to use)
     */
    public List<String> getBuiltIndexes()
    {
        List<String> indexList = new ArrayList<>();

        for (Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
        {
            SecondaryIndex index = entry.getValue();

            if (index.isIndexBuilt(entry.getKey()))
                indexList.add(entry.getValue().getIndexName());
        }

        return indexList;
    }

    /**
     * @return all CFS from indexes which use a backing CFS internally (KEYS)
     */
    public Set<ColumnFamilyStore> getIndexesBackedByCfs()
    {
        Set<ColumnFamilyStore> cfsList = new HashSet<>();

        for (SecondaryIndex index: allIndexes)
        {
            ColumnFamilyStore cfs = index.getIndexCfs();
            if (cfs != null)
                cfsList.add(cfs);
        }

        return cfsList;
    }

    /**
     * @return all indexes which do *not* use a backing CFS internally
     */
    public Set<SecondaryIndex> getIndexesNotBackedByCfs()
    {
        // we use identity map because per row indexes use same instance across many columns
        Set<SecondaryIndex> indexes = Collections.newSetFromMap(new IdentityHashMap<SecondaryIndex, Boolean>());
        for (SecondaryIndex index: allIndexes)
            if (index.getIndexCfs() == null)
                indexes.add(index);
        return indexes;
    }

    /**
     * @return all of the secondary indexes without distinction to the (non-)backed by secondary ColumnFamilyStore.
     */
    public Set<SecondaryIndex> getIndexes()
    {
        return allIndexes;
    }

    /**
     * @return if there are ANY indexes for this table..
     */
    public boolean hasIndexes()
    {
        return !indexesByColumn.isEmpty();
    }

    /**
     * When building an index against existing data, add the given partition to the index
     */
    public void indexPartition(AtomIterator atoms, OpOrder.Group opGroup, Set<SecondaryIndex> allIndexes)
    {
        Set<PerRowSecondaryIndex> perRowIndexes = perRowIndexes();
        Set<PerColumnSecondaryIndex> perColumnIndexes = perColumnIndexes();

        if (!perRowIndexes.isEmpty())
        {
            // Update entire row only once per row level index
            Set<Class<? extends SecondaryIndex>> appliedRowLevelIndexes = new HashSet<>();
            for (PerRowSecondaryIndex index : perRowIndexes)
            {
                if (appliedRowLevelIndexes.add(index.getClass()))
                    ((PerRowSecondaryIndex)index).index(atoms.partitionKey().getKey(), atoms);
            }
        }

        if (!perColumnIndexes.isEmpty())
        {
            DecoratedKey key = atoms.partitionKey();
            for (PerColumnSecondaryIndex index : perColumnIndexes)
            {
                indexRow(key, atoms.staticRow(), index, opGroup, atoms.nowInSec());
                Iterator<Row> iter = AtomIterators.asRowIterator(atoms);
                while (iter.hasNext())
                    indexRow(key, iter.next(), index, opGroup, atoms.nowInSec());
            }
        }
    }

    private void indexRow(DecoratedKey key, Row row, PerColumnSecondaryIndex index, OpOrder.Group opGroup, int nowInSec)
    {
        Clustering clustering = row.clustering();
        index.maybeIndex(key.getKey(), clustering, row.maxLiveTimestamp(), row.partitionKeyLivenessInfo().ttl(), opGroup, nowInSec);
        for (Cell cell : row)
        {
            if (!index.indexes(cell.column()))
                continue;

            if (cell.isLive(nowInSec))
                index.insert(key.getKey(), clustering, cell, opGroup, nowInSec);
        }
    }

    /**
     * Delete all data from all indexes for this partition.  For when cleanup rips a partition out entirely.
     */
    public void deleteFromIndexes(AtomIterator partition, OpOrder.Group opGroup)
    {
        ByteBuffer key = partition.partitionKey().getKey();

        for (PerRowSecondaryIndex index : perRowIndexes())
            index.delete(key, opGroup);

        Set<PerColumnSecondaryIndex> indexes = perColumnIndexes();

        while (partition.hasNext())
        {
            Atom atom = partition.next();
            if (atom.kind() != Atom.Kind.ROW)
                continue;

            Row row = (Row)atom;
            Clustering clustering = row.clustering();
            if (!row.deletion().isLive())
                for (PerColumnSecondaryIndex index : indexes)
                    index.maybeDelete(key, clustering, row.deletion(), opGroup, partition.nowInSec());
            for (Cell cell : row)
            {
                for (PerColumnSecondaryIndex index : indexes)
                {
                    if (!index.indexes(cell.column()))
                        continue;

                    ((PerColumnSecondaryIndex) index).deleteForCleanup(key, clustering, cell, opGroup, partition.nowInSec());
                }
            }
        }
    }

    /**
     * This helper acts as a closure around the indexManager and updated data
     * to ensure that down in Memtable's ColumnFamily implementation, the index
     * can get updated.
     */
    public Updater updaterFor(PartitionUpdate update, OpOrder.Group opGroup)
    {
        return (indexesByColumn.isEmpty() && rowLevelIndexMap.isEmpty())
                ? nullUpdater
                : new StandardUpdater(update, opGroup);
    }

    /**
     * Updated closure with only the modified row key.
     */
    public Updater gcUpdaterFor(DecoratedKey key, int nowInSec)
    {
        return new GCUpdater(key, nowInSec);
    }

    /**
     * Get a list of IndexSearchers from the union of expression index types
     * @param clause the query clause
     * @return the searchers needed to query the index
     */
    public List<SecondaryIndexSearcher> getIndexSearchersFor(ReadCommand command)
    {
        Map<String, Set<ColumnDefinition>> groupByIndexType = new HashMap<>();

        //Group columns by type
        for (ColumnFilter.Expression e : command.columnFilter())
        {
            SecondaryIndex index = getIndexForColumn(e.column());

            if (index == null || !index.supportsOperator(e.operator()))
                continue;

            Set<ColumnDefinition> columns = groupByIndexType.get(index.indexTypeForGrouping());

            if (columns == null)
            {
                columns = new HashSet<>();
                groupByIndexType.put(index.indexTypeForGrouping(), columns);
            }

            columns.add(e.column());
        }

        List<SecondaryIndexSearcher> indexSearchers = new ArrayList<>(groupByIndexType.size());

        //create searcher per type
        for (Set<ColumnDefinition> column : groupByIndexType.values())
            indexSearchers.add(getIndexForColumn(column.iterator().next()).createSecondaryIndexSearcher(column));

        return indexSearchers;
    }

    public SecondaryIndexSearcher getBestIndexSearcherFor(ReadCommand command)
    {
        List<SecondaryIndexSearcher> indexSearchers = getIndexSearchersFor(command);

        if (indexSearchers.isEmpty())
            return null;

        SecondaryIndexSearcher mostSelective = null;
        long bestEstimate = Long.MAX_VALUE;
        for (SecondaryIndexSearcher searcher : indexSearchers)
        {
            SecondaryIndex highestSelectivityIndex = searcher.highestSelectivityIndex(command.columnFilter());
            long estimate = highestSelectivityIndex.estimateResultRows();
            if (estimate <= bestEstimate)
            {
                bestEstimate = estimate;
                mostSelective = searcher;
            }
        }
        return mostSelective;
    }

    /**
     * Validates an union of expression index types. It will throw an {@link InvalidRequestException} if
     * any of the expressions in the provided clause is not valid for its index implementation.
     * @param filter the filter to check
     * @throws org.apache.cassandra.exceptions.InvalidRequestException in case of validation errors
     */
    public void validateFilter(ColumnFilter filter) throws InvalidRequestException
    {
        for (ColumnFilter.Expression expression : filter)
        {
            SecondaryIndex index = getIndexForColumn(expression.column());
            if (index != null && index.supportsOperator(expression.operator()))
                expression.validateForIndexing();
        }
    }

    public Set<SecondaryIndex> getIndexesByNames(Set<String> idxNames)
    {
        Set<SecondaryIndex> result = new HashSet<>();
        for (SecondaryIndex index : allIndexes)
            if (idxNames.contains(index.getIndexName()))
                result.add(index);
        return result;
    }

    public SecondaryIndex getIndexByName(String idxName)
    {
        for (SecondaryIndex index : allIndexes)
            if (idxName.equals(index.getIndexName()))
                return index;

        return null;
    }

    public void setIndexBuilt(Set<String> idxNames)
    {
        for (SecondaryIndex index : getIndexesByNames(idxNames))
            index.setIndexBuilt();
    }

    public void setIndexRemoved(Set<String> idxNames)
    {
        for (SecondaryIndex index : getIndexesByNames(idxNames))
            index.setIndexRemoved();
    }

    public void validate(Clustering clustering) throws InvalidRequestException
    {
        for (SecondaryIndex index : perColumnIndexes())
            index.validate(clustering);
    }

    public void validate(ColumnDefinition column, ByteBuffer value, CellPath path) throws InvalidRequestException
    {
        for (SecondaryIndex index : indexFor(column))
            index.validate(value, path);
    }

    static boolean shouldCleanupOldValue(Cell oldCell, Cell newCell)
    {
        // If either the value or timestamp is different, then we
        // should delete from the index. If not, then we can infer that
        // at least one of the cells is an ExpiringColumn and that the
        // difference is in the expiry time. In this case, we don't want to
        // delete the old value from the index as the tombstone we insert
        // will just hide the inserted value.
        // Completely identical cells (including expiring columns with
        // identical ttl & localExpirationTime) will not get this far due
        // to the oldCell.equals(newCell) in StandardUpdater.update
        return !oldCell.value().equals(newCell.value())
            || oldCell.livenessInfo().timestamp() != newCell.livenessInfo().timestamp();
    }

    public static interface Updater
    {
        /** Called when a row with the provided clustering and row infos is inserted */
        public void maybeIndex(Clustering clustering, long maxLiveTimestamp, int ttl, DeletionTime deletion);

        /** called when constructing the index against pre-existing data */
        public void insert(Clustering clustering, Cell cell);

        /** called when updating the index from a memtable */
        public void update(Clustering clustering, Cell oldCell, Cell cell);

        /** called when lazy-updating the index during compaction (CASSANDRA-2897) */
        public void remove(Clustering clustering, Cell current);

        /** called after memtable updates are complete (CASSANDRA-5397) */
        public void updateRowLevelIndexes();
    }

    private final class GCUpdater implements Updater
    {
        private final DecoratedKey key;
        private final int nowInSec;

        public GCUpdater(DecoratedKey key, int nowInSec)
        {
            this.key = key;
            this.nowInSec = nowInSec;
        }

        public void maybeIndex(Clustering clustering, long maxLiveTimestamp, int ttl, DeletionTime deletion)
        {
            throw new UnsupportedOperationException();
        }

        public void insert(Clustering clustering, Cell cell)
        {
            throw new UnsupportedOperationException();
        }

        public void update(Clustering clustering, Cell oldCell, Cell newCell)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(Clustering clustering, Cell cell)
        {
            for (SecondaryIndex index : indexFor(cell.column()))
            {
                if (index instanceof PerColumnSecondaryIndex)
                {
                    try (OpOrder.Group opGroup = baseCfs.keyspace.writeOrder.start())
                    {
                        ((PerColumnSecondaryIndex) index).delete(key.getKey(), clustering, cell, opGroup, nowInSec);
                    }
                }
            }
        }

        public void updateRowLevelIndexes()
        {
            for (SecondaryIndex index : rowLevelIndexMap.values())
                ((PerRowSecondaryIndex) index).index(key.getKey(), (PartitionUpdate)null);
        }
    }

    private final class StandardUpdater implements Updater
    {
        private final PartitionUpdate update;
        private final OpOrder.Group opGroup;

        public StandardUpdater(PartitionUpdate update, OpOrder.Group opGroup)
        {
            this.update = update;
            this.opGroup = opGroup;
        }

        public void maybeIndex(Clustering clustering, long maxLiveTimestamp, int ttl, DeletionTime deletion)
        {
            for (PerColumnSecondaryIndex index : perColumnIndexes())
            {
                if (maxLiveTimestamp != LivenessInfo.NO_TIMESTAMP)
                    index.maybeIndex(update.partitionKey().getKey(), clustering, maxLiveTimestamp, ttl, opGroup, update.nowInSec());
                if (!deletion.isLive())
                    index.maybeDelete(update.partitionKey().getKey(), clustering, deletion, opGroup, update.nowInSec());
            }
        }

        public void insert(Clustering clustering, Cell cell)
        {
            if (!cell.isLive(update.nowInSec()))
                return;

            for (SecondaryIndex index : indexFor(cell.column()))
                if (index instanceof PerColumnSecondaryIndex)
                    ((PerColumnSecondaryIndex) index).insert(update.partitionKey().getKey(), clustering, cell, opGroup, update.nowInSec());
        }

        public void update(Clustering clustering, Cell oldCell, Cell cell)
        {
            if (oldCell.equals(cell))
                return;

            int nowInSec = update.nowInSec();
            for (SecondaryIndex index : indexFor(cell.column()))
            {
                if (index instanceof PerColumnSecondaryIndex)
                {
                    if (cell.isLive(nowInSec))
                    {
                        ((PerColumnSecondaryIndex) index).update(update.partitionKey().getKey(), clustering, oldCell, cell, opGroup, nowInSec);
                    }
                    else
                    {
                        // Usually we want to delete the old value from the index, except when
                        // name/value/timestamp are all equal, but the columns themselves
                        // are not (as is the case when overwriting expiring columns with
                        // identical values and ttl) Then, we don't want to delete as the
                        // tombstone will hide the new value we just inserted; see CASSANDRA-7268
                        if (shouldCleanupOldValue(oldCell, cell))
                            ((PerColumnSecondaryIndex) index).delete(update.partitionKey().getKey(), clustering, oldCell, opGroup, nowInSec);
                    }
                }
            }
        }

        public void remove(Clustering clustering, Cell cell)
        {
            throw new UnsupportedOperationException();
        }

        public void updateRowLevelIndexes()
        {
            for (SecondaryIndex index : rowLevelIndexMap.values())
                ((PerRowSecondaryIndex) index).index(update.partitionKey().getKey(), update);
        }

    }
}
