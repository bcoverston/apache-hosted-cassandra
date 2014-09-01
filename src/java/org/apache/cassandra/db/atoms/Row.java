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
package org.apache.cassandra.db.atoms;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;

/**
 * Storage engine representation of a row.
 *
 * A row is identified by it's clustering column values (it's an Atom),
 * has row level informations (deletion and partition key liveness infos (see below))
 * and contains data (Cells) regarding the columns it contains.
 *
 * A row implements {@code WithLivenessInfo} and has thus a timestamp, ttl and
 * local deletion time. Those information do not apply to the row content, they
 * apply to the partition key columns. In other words, the timestamp is the
 * timestamp for the partition key columns: it is what allows to distinguish
 * between a dead row, and a live row but for which only the partition key columns
 * are set. The ttl and local deletion time information are for the case where
 * a TTL is set on those partition key columns. Note however that a row can have
 * live cells but no partition key columns timestamp, because said timestamp (and
 * its corresponding ttl) is only set on INSERT (not UPDATE).
 */
public interface Row extends Atom, Iterable<Cell>, Aliasable<Row>
{
    /**
     * The clustering values for this row.
     */
    @Override
    public Clustering clustering();

    /**
     * The columns this row contains.
     *
     * Note that this is actually a superset of the columns the row contains. The row
     * may not have values for each of those columns, but it can't have values for other
     * columns.
     *
     * @return a superset of the columns contained in this row.
     */
    public Columns columns();

    /**
     * The row deletion.
     *
     * This correspond to the last row deletion done on this row.
     *
     * @return the row deletion.
     */
    public DeletionTime deletion();

    /**
     * Liveness information for the partition key columns of this row.
     * <p>
     * As a row is uniquely identified by its partition key, all its partition key columns
     * share the same {@code LivenessInfo}. This liveness information is what allows us
     * to distinguish between a dead row (it has no live cells and its partition key liveness
     * info has no timestamp) and a live row but where all non PK columns are null (it has no
     * live cells, but its partition key liveness has a timestamp). Please note that the ttl
     * (and local deletion time) of the partition key liveness information only apply to the
     * liveness info timestamp, and not to the content of the row. Also note that because
     * in practice there is not way to only delete the partition key columns (without deleting
     * the row itself), the returned {@code LivenessInfo} can only have a local deletion time
     * if it has a TTL.
     * <p>
     * Lastly, note that it is possible for a row to have live cells but no partition key liveness
     * info timestamp, because said timestamp is only set on {@code INSERT} (which makes sense
     * in itself, see #6782) but live cells can be add through {@code UPDATE} even if the row
     * wasn't pre-existing (which users are encouraged not to do, but we can't validate).
     */
    public LivenessInfo partitionKeyLivenessInfo();

    /**
     * For the row this object represents, the biggest timestamp for a live data in the source this
     * objects originate from.
     * <p>
     * The returned timestamp should account for any live data the source this row originate from
     * knows about, not just the live data contained by the row instance itself. What this means
     * in practice is that if this row object originate from querying a memtable or a sstable, then
     * it should be the max live timestamp for any live cells stored by the memtable/sstable (for
     * the row represented by this object), even cells that are not contained by this row object
     * (because the query doesn't ask for those cells).
     * <p>
     * This is what we use to decide if the row has live data or not. We need this because the
     * CQL semantic is that a row is live as long as it has some live data, and this is the case even
     * if said live data is not selected by the query .
     *
     * @return the biggest timestamp for the live data of the CQL row represented by this object,
     * as known by the source of this object.
     */
    public long maxLiveTimestamp();

    /**
     * The time in seconds used as "now" by this row.
     * <p>
     * This is, in particular, the time used in the computation of {@link #maxLiveTimestamp} to decide
     * what is live or not (in presence of expiring data).
     *
     * @return the time in seconds considered as "now" by this row.
     */
    public int nowInSec();

    /**
     * Whether the row correspond to a static row or not.
     *
     * @return whether the row correspond to a static row or not.
     */
    public boolean isStatic();

    /**
     * Whether the row has no information whatsoever. This means no row infos
     * (timestamp, ttl, deletion, maxLiveTimestamp), no cells and no complex
     * deletion info.
     *
     * @return {@code true} if the row has no data whatsoever, {@code false} otherwise.
     */
    public boolean isEmpty();

    /**
     * Whether the row has some live information (i.e. it's not just deletion informations).
     */
    public boolean hasLiveData();

    /**
     * Whether or not this row contains any deletion for a complex column. That is if
     * there is at least one column for which {@code getDeletion} returns a non
     * live deletion time.
     */
    public boolean hasComplexDeletion();

    /**
     * Returns a cell for a simple column.
     *
     * Calls to this method are allowed to return the same Cell object, and hence the returned
     * object is only valid until the next getCell/getCells call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the Cell object
     * longer than that.
     *
     * @param c the simple column for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c);

    /**
     * Return a cell for a given complex column and cell path.
     *
     * Calls to this method are allowed to return the same Cell object, and hence the returned
     * object is only valid until the next getCell/getCells call on the same Row object. You will need
     * to copy the returned data if you plan on using a reference to the Cell object
     * longer than that.
     *
     * @param c the complex column for which to fetch the cell.
     * @param path the cell path for which to fetch the cell.
     * @return the corresponding cell or {@code null} if the row has no such cell.
     */
    public Cell getCell(ColumnDefinition c, CellPath path);

    /**
     * Returns an iterator on the cells of a complex column c.
     *
     * Calls to this method are allowed to return the same iterator object, and
     * hence the returned object is only valid until the next getCell/getCells call
     * on the same Row object. You will need to copy the returned data if you
     * plan on using a reference to the Cell object longer than that.
     *
     * @param c the complex column for which to fetch the cells.
     * @return an iterator on the cells of complex column {@code c} or {@code null} if the row has no
     * cells for that column.
     */
    public Iterator<Cell> getCells(ColumnDefinition c);

    /**
     * Deletion informations for complex columns.
     *
     * @param c the complex column for which to fetch deletion info.
     * @return the deletion time for complex column {@code c} in this row.
     */
    public DeletionTime getDeletion(ColumnDefinition c);

    /**
     * An iterator over the cells of this row.
     *
     * The iterator guarantees that for 2 rows of the same partition, columns
     * are returned in a consistent order in the sense that if the cells for
     * column c1 is returned before the cells for column c2 by the first iterator,
     * it is also the case for the 2nd iterator.
     *
     * The object returned by a call to next() is only guaranteed to be valid until
     * the next call to hasNext() or next(). If a consumer wants to keep a
     * reference on the returned Cell objects for longer than the iteration, it must
     * make a copy of it explicitly.
     *
     * @return an iterator over the cells of this row.
     */
    public Iterator<Cell> iterator();

    /**
     * Copy this row to the provided writer.
     *
     * @param writer the row writer to write this row to.
     */
    public void copyTo(Row.Writer writer);

    public String toString(CFMetaData metadata, boolean fullDetails);

    /**
     * Interface for writing a row.
     * <p>
     * Clients of this interface should abid to the following assumptions:
     *   1) if the row has a non empty clustering (it's not a static one and it doesn't belong to a table without
     *      clustering columns), then that clustering should be the first thing written (through
     *      {@link ClusteringPrefix.Writer#writeClusteringValue})).
     *   2) for a given complex column, calls to {@link #writeCell} are performed consecutively (without
     *      any call to {@code writeCell} for another column intermingled) and in {@code CellPath} order.
     *   3) {@link #endOfRow} is always called to end the writing of a given row.
     */
    public interface Writer extends ClusteringPrefix.Writer
    {
        /**
         * Writes the livness information for the partition key columns of this row.
         *
         * This call is optional: skipping it is equivalent to calling {@code writePartitionKeyLivenessInfo(LivenessInfo.NONE)}.
         *
         * @param info the liveness information for the partition key columns of the written row.
         */
        public void writePartitionKeyLivenessInfo(LivenessInfo info);

        /**
         * Writes the deletion information for this row.
         *
         * This call is optional and can be skipped if the row is not deleted.
         *
         * @param deletion the row deletion time, or {@code DeletionTime.LIVE} if the row isn't deleted.
         */
        public void writeRowDeletion(DeletionTime deletion);

        /**
         * Writes a cell to the writer.
         *
         * As mentionned above, add cells for a given column should be added consecutively (and in {@code CellPath} order for complex columns).
         *
         * @param column the column for the written cell.
         * @param isCounter whether or not this is a counter cell.
         * @param value the value for the cell. For tombstones, which don't have values, this should be an empty buffer.
         * @param info the cell liveness information.
         * @param path the {@link CellPath} for complex cells and {@code null} for regular cells.
         */
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path);

        /**
         * Writes a deletion for a complex column, that is one that apply to all cells of the complex column.
         *
         * @param column the (complex) column this is a deletion for.
         * @param complexDeletion the deletion time.
         */
        public void writeComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion);

        /**
         * Writes the maximum live timestamp for that row.
         *
         * See {@link Row#maxLiveTimestamp} for more detail on what this mean.
         */
        public void writeMaxLiveTimestamp(long maxLiveTimestamp);

        /**
         * Should be called to indicates that the row has been fully written.
         */
        public void endOfRow();
    }
}
