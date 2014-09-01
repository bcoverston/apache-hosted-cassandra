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
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Static utilities to work on RangeTombstoneMarker objects.
 */
public abstract class RangeTombstoneMarkers
{
    private RangeTombstoneMarkers() {}

    public static class Merger
    {
        private final CFMetaData metadata;
        private final AtomIterators.MergeListener listener;
        private final DeletionTime partitionDeletion;

        private Slice.Bound bound;
        private final RangeTombstoneMarker[] markers;

        // Stores for each iterator, what is the currently open marker
        private final DeletionTimeArray openMarkers;
        private final DeletionTimeArray.Cursor openMarkersCursor = new DeletionTimeArray.Cursor();

        // The index in openMarkers of the "biggest" marker. This is the last open marker
        // that has been returned for the merge.
        private int openMarker = -1;

        // As reusable marker to return the result
        private final ReusableRangeTombstoneMarker reusableMarker;

        public Merger(CFMetaData metadata, int size, DeletionTime partitionDeletion, AtomIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.listener = listener;
            this.partitionDeletion = partitionDeletion;

            this.markers = new RangeTombstoneMarker[size];
            this.openMarkers = new DeletionTimeArray(size);
            this.reusableMarker = new ReusableRangeTombstoneMarker(metadata.clusteringColumns().size());
        }

        public void clear()
        {
            Arrays.fill(markers, null);
        }

        public void add(int i, RangeTombstoneMarker marker)
        {
            bound = marker.clustering();
            markers[i] = marker;
        }

        public AtomIterators.MergedAtom merge(AtomIterators.MergedAtom merged)
        {
            if (bound.kind().isStart())
                return mergeOpenMarkers(merged);
            else
                return mergeCloseMarkers(merged);
        }

        public AtomIterators.MergedAtom mergeOpenMarkers(AtomIterators.MergedAtom merged)
        {
            int toReturn = -1;
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // We can completely ignore any marker that is shadowed by a partition level deletion
                if (partitionDeletion.supersedes(marker.deletionTime()))
                    continue;

                // We have an open marker. It's only present after merge if it's bigger than the
                // currently open marker.
                DeletionTime dt = marker.deletionTime();
                openMarkers.set(i, dt);

                if (openMarker < 0 || !openMarkers.supersedes(openMarker, dt))
                    openMarker = toReturn = i;
            }

            if (toReturn < 0)
                return merged.setTo(null);

            openMarkersCursor.setTo(openMarkers, toReturn);
            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(bound, openMarkersCursor, markers);
            return merged.setTo(reusableMarker.setTo(bound, openMarkersCursor));
        }

        public AtomIterators.MergedAtom mergeCloseMarkers(AtomIterators.MergedAtom merged)
        {
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // Close the marker for this iterator
                openMarkers.clear(i);

                // What we do now depends on what the current open marker is. If it's not i, then we can
                // just ignore that close (the corresponding open marker has been ignored).
                // If it's i, we need to issue a close for that marker. However, we also need to find the
                // next biggest open marker. If there is none, then we're good, but otherwise, on top
                // of closing the marker, we need to open that new biggest marker.
                if (i == openMarker)
                {
                    if (listener != null)
                        listener.onMergedRangeTombstoneMarkers(bound, marker.deletionTime(), markers);

                    merged.setTo(reusableMarker.setTo(bound, marker.deletionTime()));

                    // We've cleaned i so updateOpenMarker will return the new biggest one
                    updateOpenMarker();

                    if (openMarker >= 0)
                    {
                        openMarkersCursor.setTo(openMarkers, openMarker);
                        Slice.Bound openingBound = bound.withNewKind(ClusteringPrefix.Kind.INCL_START_BOUND);
                        if (listener != null)
                            listener.onMergedRangeTombstoneMarkers(openingBound, openMarkersCursor, markers);

                        merged.setSecondTo(reusableMarker.setTo(openingBound, openMarkersCursor));
                    }
                }
            }
            return merged;
        }

        public DeletionTime activeDeletion()
        {
            // Note that we'll only have an openMarker if it supersedes the partition deletion
            return openMarker < 0 ? partitionDeletion : openMarkersCursor.setTo(openMarkers, openMarker);
        }

        private void updateOpenMarker()
        {
            openMarker = -1;
            for (int i = 0; i < openMarkers.size(); i++)
            {
                if (openMarkers.isLive(i) && (openMarker < 0 || openMarkers.supersedes(i, openMarker)))
                    openMarker = i;
            }
        }
    }
}
