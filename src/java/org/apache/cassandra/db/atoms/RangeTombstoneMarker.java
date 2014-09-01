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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * A marker for a range tombsone bound.
 *
 * It can be either the opening or the closing of a range tombstone.
 * Each marker also include the associated deletion time.
 */
public interface RangeTombstoneMarker extends Atom
{
    @Override
    public Slice.Bound clustering();
    public DeletionTime deletionTime();

    public void copyTo(RangeTombstoneMarker.Writer writer);

    public interface Writer extends Slice.Bound.Writer
    {
        public void writeMarkerDeletion(DeletionTime deletion);
        public void endOfMarker();
    }
}
