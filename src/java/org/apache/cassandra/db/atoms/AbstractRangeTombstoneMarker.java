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
import java.security.MessageDigest;
import java.util.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

public abstract class AbstractRangeTombstoneMarker implements RangeTombstoneMarker
{
    public Atom.Kind kind()
    {
        return Atom.Kind.RANGE_TOMBSTONE_MARKER;
    }

    public void copyTo(RangeTombstoneMarker.Writer writer)
    {
        Slice.Bound bound = clustering();
        for (int i = 0; i < bound.size(); i++)
            writer.writeClusteringValue(bound.get(i));
        writer.writeBoundKind(bound.kind());
        writer.writeMarkerDeletion(deletionTime());
        writer.endOfMarker();
    }

    public void digest(MessageDigest digest)
    {
        clustering().digest(digest);
        deletionTime().digest(digest);
    }

    public void validateData(CFMetaData metadata)
    {
        Slice.Bound bound = clustering();
        for (int i = 0; i < bound.size(); i++)
        {
            ByteBuffer value = bound.get(i);
            if (value != null)
                metadata.comparator.subtype(i).validate(value);
        }
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Marker ");
        sb.append(clustering().toString(metadata));
        sb.append("@").append(deletionTime().markedForDeleteAt());
        return sb.toString();
    }

    public String toString(CFMetaData metadata, boolean fullDetails)
    {
        return toString(metadata);
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof RangeTombstoneMarker))
            return false;

        RangeTombstoneMarker that = (RangeTombstoneMarker)other;
        return this.clustering().equals(that.clustering())
            && this.deletionTime().equals(that.deletionTime());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clustering(), deletionTime());
    }
}
