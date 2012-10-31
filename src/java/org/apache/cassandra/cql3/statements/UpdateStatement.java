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

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.operations.ColumnOperation;
import org.apache.cassandra.cql3.operations.Operation;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql.QueryProcessor.validateKey;
import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private CFDefinition cfDef;
    private final List<Pair<ColumnIdentifier, Operation>> columns;
    private final List<ColumnIdentifier> columnNames;
    private final List<Operation> columnOperations;
    private final List<Relation> whereClause;

    private final ArrayListMultimap<CFDefinition.Name, Operation> processedColumns = ArrayListMultimap.create();
    private final Map<ColumnIdentifier, List<Term>> processedKeys = new HashMap<ColumnIdentifier, List<Term>>();

    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     *
     * @param name column family being operated on
     * @param columns a map of column name/values pairs
     * @param whereClause the where clause
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(CFName name,
                           List<Pair<ColumnIdentifier, Operation>> columns,
                           List<Relation> whereClause,
                           Attributes attrs)
    {
        super(name, attrs);

        this.columns = columns;
        this.whereClause = whereClause;
        this.columnNames = null;
        this.columnOperations = null;
    }

    /**
     * Creates a new UpdateStatement from a column family name, a consistency level,
     * key, and lists of column names and values.  It is intended for use with the
     * alternate update format, <code>INSERT</code>.
     *
     * @param name column family being operated on
     * @param columnNames list of column names
     * @param columnOperations list of column 'set' operations (corresponds to names)
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    public UpdateStatement(CFName name,
                           Attributes attrs,
                           List<ColumnIdentifier> columnNames,
                           List<Operation> columnOperations)
    {
        super(name, attrs);

        this.columnNames = columnNames;
        this.columnOperations = columnOperations;
        this.whereClause = null;
        this.columns = null;
    }

    protected void validateConsistency(ConsistencyLevel cl) throws InvalidRequestException
    {
        if (type == Type.COUNTER)
            cl.validateCounterForWrite(cfDef.cfm);
        else
            cl.validateForWrite(cfDef.cfm.ksName);
    }

    /** {@inheritDoc} */
    public Collection<IMutation> getMutations(ClientState clientState, List<ByteBuffer> variables, boolean local, ConsistencyLevel cl, long now)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildKeyNames(cfDef, processedKeys, variables);

        ColumnNameBuilder builder = cfDef.getColumnNameBuilder();
        buildColumnNames(cfDef, processedKeys, builder, variables, true);

        // Lists SET operation incurs a read.
        List<ByteBuffer> toRead = null;
        for (Map.Entry<CFDefinition.Name, Operation> entry : processedColumns.entries())
        {
            CFDefinition.Name name = entry.getKey();
            Operation value = entry.getValue();

            if (!(name.type instanceof ListType))
                continue;

            if (value.requiresRead())
            {
                if (toRead == null)
                    toRead = new ArrayList<ByteBuffer>();
                toRead.add(name.name.key);
                break;
            }
        }

        Map<ByteBuffer, ColumnGroupMap> rows = toRead != null ? readRows(keys, builder, toRead, (CompositeType)cfDef.cfm.comparator, local, cl) : null;

        Collection<IMutation> mutations = new LinkedList<IMutation>();
        UpdateParameters params = new UpdateParameters(variables, getTimestamp(now), getTimeToLive());

        for (ByteBuffer key: keys)
            mutations.add(mutationForKey(cfDef, key, builder, params, rows == null ? null : rows.get(key), cl));

        return mutations;
    }

    // Returns the first empty component or null if none are
    static CFDefinition.Name buildColumnNames(CFDefinition cfDef, Map<ColumnIdentifier, List<Term>> processed, ColumnNameBuilder builder, List<ByteBuffer> variables, boolean requireAllComponent)
    throws InvalidRequestException
    {
        CFDefinition.Name firstEmpty = null;
        for (CFDefinition.Name name : cfDef.columns.values())
        {
            List<Term> values = processed.get(name.name);
            if (values == null || values.isEmpty())
            {
                firstEmpty = name;
                if (requireAllComponent && cfDef.isComposite && !cfDef.isCompact)
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));
            }
            else if (firstEmpty != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmpty.name, name.name));
            }
            else
            {
                assert values.size() == 1; // We only allow IN for row keys so far
                builder.add(values.get(0), Relation.Type.EQ, variables);
            }
        }
        return firstEmpty;
    }

    static List<ByteBuffer> buildKeyNames(CFDefinition cfDef, Map<ColumnIdentifier, List<Term>> processed, List<ByteBuffer> variables)
    throws InvalidRequestException
    {
        ColumnNameBuilder keyBuilder = cfDef.getKeyNameBuilder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (CFDefinition.Name name : cfDef.keys.values())
        {
            List<Term> values = processed.get(name.name);
            if (values == null || values.isEmpty())
                throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", name));

            if (keyBuilder.remainingCount() == 1)
            {
                for (Term t : values)
                    keys.add(keyBuilder.copy().add(t, Relation.Type.EQ, variables).build());
            }
            else
            {
                if (values.size() > 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                keyBuilder.add(values.get(0), Relation.Type.EQ, variables);
            }
        }
        return keys;
    }

    /**
     * Compute a row mutation for a single key
     *
     * @return row mutation
     *
     * @throws InvalidRequestException on the wrong request
     */
    private IMutation mutationForKey(CFDefinition cfDef, ByteBuffer key, ColumnNameBuilder builder, UpdateParameters params, ColumnGroupMap group, ConsistencyLevel cl)
    throws InvalidRequestException
    {
        validateKey(key);
        // if true we need to wrap RowMutation into CounterMutation
        boolean hasCounterColumn = false;

        QueryProcessor.validateKey(key);
        RowMutation rm = new RowMutation(cfDef.cfm.ksName, key);
        ColumnFamily cf = rm.addOrGet(cfDef.cfm.cfName);

        // Inserting the CQL row marker (see #4361)
        // We always need to insert a marker, because of the following situation:
        //   CREATE TABLE t ( k int PRIMARY KEY, c text );
        //   INSERT INTO t(k, c) VALUES (1, 1)
        //   DELETE c FROM t WHERE k = 1;
        //   SELECT * FROM t;
        // The last query should return one row (but with c == null). Adding
        // the marker with the insert make sure the semantic is correct (while making sure a
        // 'DELETE FROM t WHERE k = 1' does remove the row entirely)
        if (cfDef.isComposite && !cfDef.isCompact)
        {
            ByteBuffer name = builder.copy().add(ByteBufferUtil.EMPTY_BYTE_BUFFER).build();
            cf.addColumn(params.makeColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }

        if (cfDef.isCompact)
        {
            if (builder.componentCount() == 0)
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s", cfDef.columns.values().iterator().next()));

            Operation operation;
            if (cfDef.value == null)
            {
                // No value was defined, we set to the empty value
                operation = ColumnOperation.SetToEmpty();
            }
            else
            {
                List<Operation> operations = processedColumns.get(cfDef.value);
                if (operations.isEmpty())
                    throw new InvalidRequestException(String.format("Missing mandatory column %s", cfDef.value));
                assert operations.size() == 1;
                operation = operations.get(0);
            }
            hasCounterColumn = addToMutation(cf, builder, cfDef.value, operation, params, null);
        }
        else
        {
            for (Map.Entry<CFDefinition.Name, Operation> entry : processedColumns.entries())
            {
                CFDefinition.Name name = entry.getKey();
                Operation op = entry.getValue();
                hasCounterColumn |= addToMutation(cf, builder.copy().add(name.name.key), name, op, params, group == null || !op.requiresRead() ? null : group.getCollection(name.name.key));
            }
        }

        return (hasCounterColumn) ? new CounterMutation(rm, cl) : rm;
    }

    private boolean addToMutation(ColumnFamily cf,
                                  ColumnNameBuilder builder,
                                  CFDefinition.Name valueDef,
                                  Operation valueOperation,
                                  UpdateParameters params,
                                  List<Pair<ByteBuffer, IColumn>> list) throws InvalidRequestException
    {
        Operation.Type type = valueOperation.getType();

        switch (type)
        {
            case COLUMN:
                if (valueDef != null && valueDef.type.isCollection())
                {
                    // This means this was a prepared statement where the whole collection was provided
                    // For have to deserialize it since it will be multiple columns
                    ((ColumnOperation)valueOperation).executePreparedCollection(cf, builder.copy(), (CollectionType)valueDef.type, params);
                }
                else
                {
                    valueOperation.execute(cf, builder.copy(), valueDef == null ? null : valueDef.type, params);
                }
                break;
            case COUNTER:
                if (valueDef != null && valueDef.type.isCollection())
                    throw new InvalidRequestException("Cannot assign collection value to column with " + valueDef.type + " type.");

                valueOperation.execute(cf, builder.copy(), valueDef == null ? null : valueDef.type, params);
                break;
            default:
                if (!valueDef.type.isCollection())
                    throw new InvalidRequestException("Can't apply collection operation on column with " + valueDef.type + " type.");

                valueOperation.execute(cf, builder.copy(), (CollectionType) valueDef.type, params, list);
                break;
        }
        return valueOperation.getType() == Operation.Type.COUNTER;
    }

    public ParsedStatement.Prepared prepare(CFDefinition.Name[] boundNames) throws InvalidRequestException
    {
        if (columns != null)
        {
            for (Pair<ColumnIdentifier, Operation> column : columns)
            {
                if (column.right.getType() == Operation.Type.COUNTER)
                {
                    if (type == null)
                        type = Type.COUNTER;
                    else if (type != Type.COUNTER)
                        throw new InvalidRequestException("Mix of counter and non-counter operations is not allowed.");
                }
                else if (type == Type.COUNTER)
                {
                    throw new InvalidRequestException("Mix of counter and non-counter operations is not allowed.");
                }
            }
        }

        if (type == null)
            type = Type.LOGGED;

        // Deal here with the keyspace overwrite thingy to avoid mistake
        CFMetaData metadata = validateColumnFamily(keyspace(), columnFamily(), type == Type.COUNTER);
        cfDef = metadata.getCfDef();

        if (columns == null)
        {
            // Created from an INSERT
            // Don't hate, validate.
            if (columnNames.size() != columnOperations.size())
                throw new InvalidRequestException("unmatched column names/values");
            if (columnNames.size() < 1)
                throw new InvalidRequestException("no columns specified for INSERT");

            for (int i = 0; i < columnNames.size(); i++)
            {
                CFDefinition.Name name = cfDef.get(columnNames.get(i));
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", columnNames.get(i)));

                Operation operation = columnOperations.get(i);
                for (Term t : operation.getValues())
                    if (t.isBindMarker())
                        boundNames[t.bindIndex] = name;

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        if (processedKeys.containsKey(name.name))
                            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
                        if (operation.getType() != Operation.Type.COLUMN)
                            throw new InvalidRequestException(String.format("Invalid definition for %s, not a collection type", name));
                        processedKeys.put(name.name, operation.getValues());
                        break;
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        if (processedColumns.containsKey(name))
                            throw new InvalidRequestException(String.format("Multiple definitions found for column %s", name));
                        processedColumns.put(name, operation);
                        break;
                }
            }
        }
        else
        {
            // Created from an UPDATE
            for (Pair<ColumnIdentifier, Operation> entry : columns)
            {
                CFDefinition.Name name = cfDef.get(entry.left);
                if (name == null)
                    throw new InvalidRequestException(String.format("Unknown identifier %s", entry.left));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", entry.left));
                    case VALUE_ALIAS:
                    case COLUMN_METADATA:
                        for (Operation op : processedColumns.get(name))
                            if (op.getType() == Operation.Type.COLUMN)
                                throw new InvalidRequestException(String.format("Multiple definitions found for column %s", name));

                        Operation op = entry.right;
                        for (Term t : op.getValues())
                            if (t.isBindMarker())
                                boundNames[t.bindIndex] = name;
                        processedColumns.put(name, op);
                        break;
                }
            }
            processKeys(cfDef, whereClause, processedKeys, boundNames);
        }

        return new ParsedStatement.Prepared(this, Arrays.<ColumnSpecification>asList(boundNames));
    }

    public ParsedStatement.Prepared prepare() throws InvalidRequestException
    {
        CFDefinition.Name[] names = new CFDefinition.Name[getBoundsTerms()];
        return prepare(names);
    }

    // Reused by DeleteStatement
    static void processKeys(CFDefinition cfDef, List<Relation> keys, Map<ColumnIdentifier, List<Term>> processed, CFDefinition.Name[] names) throws InvalidRequestException
    {
        for (Relation rel : keys)
        {
            CFDefinition.Name name = cfDef.get(rel.getEntity());
            if (name == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", rel.getEntity()));

            switch (name.kind)
            {
                case KEY_ALIAS:
                case COLUMN_ALIAS:
                    List<Term> values;
                    if (rel.operator() == Relation.Type.EQ)
                        values = Collections.singletonList(rel.getValue());
                    else if (name.kind == CFDefinition.Name.Kind.KEY_ALIAS && rel.operator() == Relation.Type.IN)
                        values = rel.getInValues();
                    else
                        throw new InvalidRequestException(String.format("Invalid operator %s for key %s", rel.operator(), rel.getEntity()));

                    if (processed.containsKey(name.name))
                        throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", name));
                    for (Term value : values)
                        if (value.isBindMarker())
                            names[value.bindIndex] = name;
                    processed.put(name.name, values);
                    break;
                case VALUE_ALIAS:
                case COLUMN_METADATA:
                    throw new InvalidRequestException(String.format("PRIMARY KEY part %s found in SET part", rel.getEntity()));
            }
        }
    }

    public String toString()
    {
        return String.format("UpdateStatement(name=%s, keys=%s, columns=%s, timestamp=%s, timeToLive=%s)",
                             cfName,
                             whereClause,
                             columns,
                             isSetTimestamp() ? getTimestamp(-1) : "<now>",
                             getTimeToLive());
    }
}
