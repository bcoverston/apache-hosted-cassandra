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

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Index on the collection element of the cell name of a collection.
 *
 * The row keys for this index are given by the collection element for
 * indexed columns.
 */
public class CompositesIndexOnCollectionKey extends CompositesIndexIncludingCollectionKey
{
    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).nameComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path)
    {
        return path.get(0);
    }

    @Override
    public boolean supportsOperator(Operator operator)
    {
        return operator == Operator.CONTAINS_KEY ||
               operator == Operator.CONTAINS && columnDef.type instanceof SetType;
    }

    public boolean isStale(Row data, ByteBuffer indexValue)
    {
        Cell cell = data.getCell(columnDef, CellPath.create(indexValue));
        return cell == null || !cell.isLive(data.nowInSec());
    }
}
