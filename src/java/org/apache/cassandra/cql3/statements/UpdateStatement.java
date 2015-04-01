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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    private UpdateStatement(StatementType type, int boundTerms, CFMetaData cfm, Attributes attrs)
    {
        super(type, boundTerms, cfm, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return true;
    }

    public void addUpdateForKey(PartitionUpdate update, CBuilder cbuilder, UpdateParameters params)
    throws InvalidRequestException
    {
        if (updatesRegularRows())
        {
            Clustering clustering = cbuilder.build();
            Row.Writer writer = update.writer();
            params.writeClustering(clustering, writer);


            // We update the row timestamp (ex-row marker) only on INSERT (#6782)
            // Further, COMPACT tables semantic differs from "CQL3" ones in that a row exists only if it has
            // a non-null column, so we don't want to set the row timestamp for them. One exception however is
            // the case of a COMPACT TABLE with only a PK: we allow that because we can translate it to a thrift
            // table with a single cell having an empty value. In that case, since we don't have any column, we
            // do want to set the row timestamp.
            if (type == StatementType.INSERT && (cfm.isCQL3Table() || cfm.partitionColumns().isEmpty()))
                params.writePartitionKeyLivenessInfo(writer);

            List<Operation> updates = getRegularOperations();

            // For a dense layout, when we translate it to thrift, we don't have a row marker. So we don't accept an insert/update
            // that only sets the PK unless the is no declared non-PK columns (which means that the table definition only includes
            // the PK in the first place)
            if (updates.isEmpty() && cfm.layout().isDense() && !cfm.partitionColumns().isEmpty())
                throw new InvalidRequestException(String.format("Column %s is mandatory for this COMPACT STORAGE table", cfm.compactValueColumn().name));

            for (Operation op : updates)
                op.execute(update.partitionKey(), clustering, writer, params);

            writer.endOfRow();
        }

        if (updatesStaticRow())
        {
            Row.Writer writer = update.staticWriter();
            for (Operation op : getStaticOperations())
                op.execute(update.partitionKey(), Clustering.STATIC_CLUSTERING, writer, params);
            writer.endOfRow();
        }
    }

    public static class ParsedInsert extends ModificationStatement.Parsed
    {
        private final List<ColumnIdentifier.Raw> columnNames;
        private final List<Term.Raw> columnValues;

        /**
         * A parsed <code>INSERT</code> statement.
         *
         * @param name column family being operated on
         * @param columnNames list of column names
         * @param columnValues list of column values (corresponds to names)
         * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
         */
        public ParsedInsert(CFName name,
                            Attributes.Raw attrs,
                            List<ColumnIdentifier.Raw> columnNames, List<Term.Raw> columnValues,
                            boolean ifNotExists)
        {
            super(name, attrs, null, ifNotExists, false);
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        protected ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            UpdateStatement stmt = new UpdateStatement(ModificationStatement.StatementType.INSERT,boundNames.size(), cfm, attrs);

            // Created from an INSERT
            if (stmt.isCounter())
                throw new InvalidRequestException("INSERT statement are not allowed on counter tables, use UPDATE instead");
            if (columnNames.size() != columnValues.size())
                throw new InvalidRequestException("Unmatched column names/values");
            if (columnNames.isEmpty())
                throw new InvalidRequestException("No columns provided to INSERT");

            for (int i = 0; i < columnNames.size(); i++)
            {
                ColumnIdentifier id = columnNames.get(i).prepare(cfm);
                ColumnDefinition def = cfm.getColumnDefinition(id);
                if (def == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", id));

                for (int j = 0; j < i; j++)
                {
                    ColumnIdentifier otherId = columnNames.get(j).prepare(cfm);
                    if (id.equals(otherId))
                        throw new InvalidRequestException(String.format("Multiple definitions found for column %s", id));
                }

                Term.Raw value = columnValues.get(i);

                switch (def.kind)
                {
                    case PARTITION_KEY:
                    case CLUSTERING_COLUMN:
                        Term t = value.prepare(keyspace(), def);
                        t.collectMarkerSpecification(boundNames);
                        stmt.addKeyValue(def, t);
                        break;
                    default:
                        Operation operation = new Operation.SetValue(value).prepare(keyspace(), def);
                        operation.collectMarkerSpecification(boundNames);
                        stmt.addOperation(operation);
                        break;
                }
            }
            return stmt;
        }
    }

    public static class ParsedUpdate extends ModificationStatement.Parsed
    {
        // Provided for an UPDATE
        private final List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> updates;
        private final List<Relation> whereClause;

        /**
         * Creates a new UpdateStatement from a column family name, columns map, consistency
         * level, and key term.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (timestamp, timeToLive)
         * @param updates a map of column operations to perform
         * @param whereClause the where clause
         * @param ifExists flag to check if row exists
         * */
        public ParsedUpdate(CFName name,
                            Attributes.Raw attrs,
                            List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> updates,
                            List<Relation> whereClause,
                            List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions,
                            boolean ifExists)
        {
            super(name, attrs, conditions, false, ifExists);
            this.updates = updates;
            this.whereClause = whereClause;
        }

        protected ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException
        {
            UpdateStatement stmt = new UpdateStatement(ModificationStatement.StatementType.UPDATE, boundNames.size(), cfm, attrs);

            for (Pair<ColumnIdentifier.Raw, Operation.RawUpdate> entry : updates)
            {
                ColumnDefinition def = cfm.getColumnDefinition(entry.left.prepare(cfm));
                if (def == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                Operation operation = entry.right.prepare(keyspace(), def);
                operation.collectMarkerSpecification(boundNames);

                switch (def.kind)
                {
                    case PARTITION_KEY:
                    case CLUSTERING_COLUMN:
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                    default:
                        stmt.addOperation(operation);
                        break;
                }
            }

            stmt.processWhereClause(whereClause, boundNames);
            return stmt;
        }
    }
}
