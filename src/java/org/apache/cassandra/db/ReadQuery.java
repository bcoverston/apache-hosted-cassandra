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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public abstract class ReadQuery
{
    /**
     * Executes the query at the provided consistency level.
     *
     * @param consistency the consistency level to achieve for the query.
     * @param clientState the {@code ClientState} for the query. In practive, this can be null unless
     * {@code consistency} is a serial consistency.
     *
     * @return the result of the query.
     */
    public abstract DataIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException;

    /**
     * Execute the query locally.
     *
     * @return the result of the query.
     */
    public abstract DataIterator executeLocally();

    /**
     * The limits for the query.
     *
     * @return The limits for the query.
     */
    public abstract DataLimits limits();

    protected abstract CFMetaData metadata();
}
