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

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.atoms.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    public static ReadResponse createDataResponse(PartitionIterator data)
    {
        try (PartitionIterator iter = data)
        {
            return new DataResponse(data);
        }
    }

    public static ReadResponse createDigestResponse(PartitionIterator data)
    {
        try (PartitionIterator iter = data)
        {
            return new DigestResponse(makeDigest(data));
        }
    }

    public static ReadResponse createLocalDataResponse(PartitionIterator data)
    {
        // We don't consume the data ourselves so we shouldn't close it.
        return new LocalDataResponse(data);
    }

    public abstract PartitionIterator makeIterator();
    public abstract ByteBuffer digest();
    public abstract boolean isDigestQuery();

    protected static ByteBuffer makeDigest(PartitionIterator iterator)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        PartitionIterators.digest(iterator, digest);
        return ByteBuffer.wrap(digest.digest());
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public PartitionIterator makeIterator()
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest()
        {
            return digest;
        }

        public boolean isDigestQuery()
        {
            return true;
        }
    }

    private static class DataResponse extends ReadResponse
    {
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final LegacyLayout.Flag flag;

        private DataResponse(ByteBuffer data)
        {
            this.data = data;
            this.flag = LegacyLayout.Flag.FROM_REMOTE;
        }

        private DataResponse(PartitionIterator iter)
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            try
            {
                PartitionIterators.serializerForIntraNode().serialize(iter, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
                this.flag = LegacyLayout.Flag.LOCAL;
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public PartitionIterator makeIterator()
        {
            try
            {
                DataInput in = new DataInputStream(ByteBufferUtil.inputStream(data));
                return PartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest()
        {
            try (PartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator);
            }
        }

        public boolean isDigestQuery()
        {
            return false;
        }
    }

    private static class LocalDataResponse extends ReadResponse
    {
        private final PartitionIterator iterator;
        private boolean returned;

        private LocalDataResponse(PartitionIterator iterator)
        {
            this.iterator = iterator;
        }

        public PartitionIterator makeIterator()
        {
            if (returned)
                throw new IllegalStateException();

            returned = true;
            return iterator;
        }

        public ByteBuffer digest()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isDigestQuery()
        {
            return false;
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            assert !(response instanceof LocalDataResponse);
            boolean isDigest = response.isDigestQuery();
            ByteBufferUtil.writeWithShortLength(isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            ByteBuffer digest = ByteBufferUtil.readWithShortLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithLength(in);
            return new DataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            TypeSizes sizes = TypeSizes.NATIVE;
            boolean isDigest = response.isDigestQuery();
            long size = ByteBufferUtil.serializedSizeWithShortLength(isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER, sizes);

            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithLength(data, sizes);
            }
            return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        out.writeInt(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            Row.serializer.serialize(row, out, version);
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int rowCount = in.readInt();
            //        List<Row> rows = new ArrayList<Row>(rowCount);
            //        for (int i = 0; i < rowCount; i++)
            //            rows.add(Row.serializer.deserialize(in, version));
            //        return new RangeSliceReply(rows);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int size = TypeSizes.NATIVE.sizeof(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            size += Row.serializer.serializedSize(row, version);
            //        return size;
        }
    }
}
