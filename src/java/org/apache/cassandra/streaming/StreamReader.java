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
package org.apache.cassandra.streaming;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import com.google.common.base.Throwables;

import com.google.common.collect.UnmodifiableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableAtomIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;


/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    protected final UUID cfId;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamSession session;
    protected final Version inputVersion;
    protected final long repairedAt;
    protected final SSTableFormat.Type format;
    protected final int sstableLevel;
    protected final SerializationHeader.Component header;

    protected Descriptor desc;

    public StreamReader(FileMessageHeader header, StreamSession session)
    {
        this.session = session;
        this.cfId = header.cfId;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
        this.inputVersion = header.version;
        this.repairedAt = header.repairedAt;
        this.format = header.format;
        this.sstableLevel = header.sstableLevel;
        this.header = header.header;
    }

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public SSTableWriter read(ReadableByteChannel channel) throws IOException
    {
        logger.debug("reading file from {}, repairedAt = {}, level = {}", session.peer, repairedAt, sstableLevel);
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        if (kscf == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + cfId + " was dropped during streaming");
        }
        ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        SSTableWriter writer = createWriter(cfs, totalSize, repairedAt, format);

        DataInputStream dis = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
        BytesReadTracker in = new BytesReadTracker(dis);
        StreamDeserializer deserializer = new StreamDeserializer(cfs.metadata, in, inputVersion, header.toHeader(cfs.metadata));
        try
        {
            while (in.getBytesRead() < totalSize)
            {
                writePartition(deserializer, writer, cfs);
                // TODO move this to BytesReadTracker
                session.progress(desc, ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }
            return writer;
        }
        catch (Throwable e)
        {
            writer.abort();
            drain(dis, in.getBytesRead());
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    protected SSTableWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, SSTableFormat.Type format) throws IOException
    {
        Directories.DataDirectory localDir = cfs.directories.getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException("Insufficient disk space to store " + totalSize + " bytes");
        desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir), format));

        return SSTableWriter.create(desc, estimatedKeys, repairedAt, sstableLevel, header.toHeader(cfs.metadata));
    }

    protected void drain(InputStream dis, long bytesRead) throws IOException
    {
        long toSkip = totalSize() - bytesRead;

        // InputStream.skip can return -1 if dis is inaccessible.
        long skipped = dis.skip(toSkip);
        if (skipped == -1)
            return;

        toSkip = toSkip - skipped;
        while (toSkip > 0)
        {
            skipped = dis.skip(toSkip);
            if (skipped == -1)
                break;
            toSkip = toSkip - skipped;
        }
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    protected void writePartition(StreamDeserializer deserializer, SSTableWriter writer, ColumnFamilyStore cfs) throws IOException
    {
        DecoratedKey key = deserializer.newPartition();
        writer.append(deserializer);
        deserializer.checkForExceptions();
        cfs.invalidateCachedPartition(key);
    }

    public static class StreamDeserializer extends UnmodifiableIterator<Atom> implements AtomIterator
    {
        private final CFMetaData metadata;
        private final DataInput in;
        private final SerializationHeader header;
        private final SerializationHelper helper;

        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private Iterator<Atom> atomIter;
        private Row staticRow;
        private IOException exception;

        public StreamDeserializer(CFMetaData metadata, DataInput in, Version version, SerializationHeader header)
        {
            this.metadata = metadata;
            this.in = in;
            this.helper = new SerializationHelper(version.correspondingMessagingVersion(), LegacyLayout.Flag.PRESERVE_SIZE, FBUtilities.nowInSeconds());
            this.header = header;
        }

        public DecoratedKey newPartition() throws IOException
        {
            key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
            partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
            staticRow = header.hasStatic()
                      ? AtomSerializer.serializer.deserializeStaticRow(in, header, helper)
                      : Rows.EMPTY_STATIC_ROW;
            atomIter = new SSTableAtomIterator(in, header, helper)
            {
                @Override
                protected Row updateRow(ReusableRow row)
                {
                    return metadata.isCounter()
                         ? row.markCounterLocalShardsToBeCleared()
                         : row;
                }

                protected RuntimeException onIOException(IOException e)
                {
                    // We'll catch that in hasNext() below
                    throw new RuntimeException(e);
                }
            };
            return key;
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public PartitionColumns columns()
        {
            // We don't know which columns we'll get so assume it can be all of them
            return metadata.partitionColumns();
        }

        public boolean isReverseOrder()
        {
            return false;
        }

        public DecoratedKey partitionKey()
        {
            return key;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public AtomStats stats()
        {
            return header.stats();
        }

        public int nowInSec()
        {
            return helper.nowInSec;
        }

        public boolean hasNext()
        {
            try
            {
                return atomIter.hasNext();
            }
            catch (RuntimeException e)
            {
                if (e.getCause() != null && e.getCause() instanceof IOException)
                {
                    exception = (IOException)e.getCause();
                    return false;
                }
                throw e;
            }
        }

        public Atom next()
        {
            // Note that in practice we know that IOException will be thrown by hasNext(), because that's
            // where the actual reading happens, so we don't bother catching RuntimeException here (contrarily
            // to what we do in hasNext)
            Atom atom = atomIter.next();
            return metadata.isCounter() && atom.kind() == Atom.Kind.ROW
                ? maybeMarkLocalToBeCleared((Row)atom)
                : atom;
        }

        private static Row maybeMarkLocalToBeCleared(Row row)
        {
            // TODO
            throw new UnsupportedOperationException();
        }

        public void checkForExceptions() throws IOException
        {
            if (exception != null)
                throw exception;
        }

        public void close()
        {
        }
    }
}
