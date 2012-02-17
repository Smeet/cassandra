/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.sstable.IndexHelper;

public class CachedDataInput extends AbstractDataInput implements FileDataInput
{
    // we keep # concurrent readers output stream to avoid array allocation
    private final static ThreadLocal<ByteArrayOutputStream> BAOS = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream(1024 * 1024);
        }
    };

    private ByteBuffer buffer;
    private int position;
    private long maxTimestamp;

    public static ByteBuffer serialize(ColumnFamily cf) {
        ByteArrayOutputStream baos = BAOS.get();
        baos.reset();
        DataOutput dos = new DataOutputStream(baos);
        // serialize max timestamp
        serializeMaxTimestamp(cf, dos);
        serializeIndex(cf, dos);
        ColumnFamily.serializer().serializeForSSTable(cf, dos);
        return ByteBuffer.wrap(baos.toByteArray());
    }

    private static void serializeMaxTimestamp(ColumnFamily cf, DataOutput dos) {
        try {
            dos.writeLong(cf.maxTimestamp());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void serializeIndex(IIterableColumns columns, DataOutput dos)
    {
        try {
            List<IndexHelper.IndexInfo> indexList = new ArrayList<IndexHelper.IndexInfo>();
            int endPosition = 0, startPosition = -1;
            int indexSizeInBytes = 0;
            IColumn lastColumn = null, firstColumn = null;
            for (IColumn column : columns)
            {
                if (firstColumn == null)
                {
                    firstColumn = column;
                    startPosition = endPosition;
                }
                endPosition += column.serializedSize();
                /* if we hit the column index size that we have to index after, go ahead and index it. */
                if (endPosition - startPosition >= DatabaseDescriptor.getColumnIndexSize())
                {
                    IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), column.name(), startPosition, endPosition - startPosition);
                    indexList.add(cIndexInfo);
                    indexSizeInBytes += cIndexInfo.serializedSize();
                    firstColumn = null;
                }

                lastColumn = column;
            }

            // all columns were GC'd after all
            if (lastColumn == null)
            {
                // always write index size
                dos.writeInt(0);
                return;
            }

            // the last column may have fallen on an index boundary already.  if not, index it explicitly.
            if (indexList.isEmpty() || columns.getComparator().compare(indexList.get(indexList.size() - 1).lastName, lastColumn.name()) != 0)
            {
                IndexHelper.IndexInfo cIndexInfo = new IndexHelper.IndexInfo(firstColumn.name(), lastColumn.name(), startPosition, endPosition - startPosition);
                indexList.add(cIndexInfo);
                indexSizeInBytes += cIndexInfo.serializedSize();
            }

            // write the index.  we should always have at least one computed index block, but we only write it out if there is more than that.
            assert indexSizeInBytes > 0;
            if (indexList.size() > 1)
            {
                dos.writeInt(indexSizeInBytes);
                for (IndexHelper.IndexInfo cIndexInfo : indexList)
                {
                    cIndexInfo.serialize(dos);
                }
            }
            else
            {
                dos.writeInt(0);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CachedDataInput(ByteBuffer buffer)
    {
        assert buffer != null;
        this.buffer = buffer;
        this.maxTimestamp = buffer.getLong(0);
        position = 8;
    }

    // don't make this public, this is only for seeking WITHIN the current mapped segment
    protected void seekInternal(int pos)
    {
        position = pos;
    }

    protected int getPosition()
    {
        return position;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public void reset(FileMark mark) throws IOException
    {
        assert mark instanceof CachedDataInputMark;
        seekInternal(((CachedDataInputMark) mark).position);
    }

    public FileMark mark()
    {
        return new CachedDataInputMark(position);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof CachedDataInputMark;
        assert position >= ((CachedDataInputMark) mark).position;
        return position - ((CachedDataInputMark) mark).position;
    }

    public boolean isEOF() throws IOException
    {
        return position == buffer.capacity();
    }

    public long bytesRemaining() throws IOException
    {
        return buffer.capacity() - position;
    }

    public String getPath()
    {
        throw new UnsupportedOperationException("cached data rows dont have a path");
    }

    public int read() throws IOException
    {
        if (isEOF())
            return -1;
        return buffer.get(position++) & 0xFF;
    }

    /**
     * Does the same thing as <code>readFully</code> do but without copying data (thread safe)
     * @param length length of the bytes to read
     * @return buffer with portion of file content
     * @throws java.io.IOException on any fail of I/O operation
     */
    public synchronized ByteBuffer readBytes(int length) throws IOException
    {
        int remaining = buffer.remaining() - position;
        if (length > remaining)
            throw new IOException(String.format("mmap segment underflow; remaining is %d but %d requested",
                    remaining, length));

        ByteBuffer bytes = buffer.duplicate();
        bytes.position(buffer.position() + position).limit(buffer.position() + position + length);
        position += length;

        return bytes;
    }



    @Override
    public final void readFully(byte[] buffer) throws IOException
    {
        throw new UnsupportedOperationException("use readBytes instead");
    }

    @Override
    public final void readFully(byte[] buffer, int offset, int count) throws IOException
    {
        throw new UnsupportedOperationException("use readBytes instead");
    }

    public int skipBytes(int n) throws IOException
    {
        assert n >= 0 : "skipping negative bytes is illegal: " + n;
        if (n == 0)
            return 0;
        int oldPosition = position;
        assert ((long)oldPosition) + n <= Integer.MAX_VALUE;
        position = Math.min(buffer.capacity(), position + n);
        return position - oldPosition;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    private static class CachedDataInputMark implements FileMark
    {
        int position;

        CachedDataInputMark(int position)
        {
            this.position = position;
        }
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "position=" + position +
                ")";
    }
}
