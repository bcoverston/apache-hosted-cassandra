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
import java.util.Arrays;

import org.apache.cassandra.db.*;

public class ReusableRangeTombstoneMarker extends AbstractRangeTombstoneMarker
{
    private final int maxClusteringSize;

    private Slice.Bound bound;
    private DeletionTime deletion;

    private ReusableWriter writer;

    public ReusableRangeTombstoneMarker(int size)
    {
        this.maxClusteringSize = size;
    }

    public ReusableRangeTombstoneMarker setTo(Slice.Bound bound, DeletionTime deletion)
    {
        this.bound = bound;
        this.deletion = deletion;
        return this;
    }

    public Slice.Bound clustering()
    {
        return bound;
    }

    public DeletionTime deletionTime()
    {
        return deletion;
    }

    public Atom takeAlias()
    {
        return new SimpleRangeTombstoneMarker(bound, deletion.takeAlias());
    }

    public Writer writer()
    {
        if (writer == null)
            writer = new ReusableWriter();
        return writer.reset();
    }

    private class ReusableWriter implements Writer
    {
        private final ByteBuffer[] values = new ByteBuffer[maxClusteringSize];
        private int size;
        private Slice.Bound.Kind kind;

        public void writeClusteringValue(ByteBuffer value)
        {
            values[size++] = value;
        }

        public void writeBoundKind(Slice.Bound.Kind kind)
        {
            this.kind = kind;
        }

        public void writeMarkerDeletion(DeletionTime deletion)
        {
            ReusableRangeTombstoneMarker.this.deletion = deletion;
        }

        public void endOfMarker()
        {
            ReusableRangeTombstoneMarker.this.bound = Slice.Bound.create(kind, Arrays.copyOfRange(values, 0, size));
        }

        private Writer reset()
        {
            Arrays.fill(values, null);
            size = 0;
            return this;
        }
    }
}
