package org.apache.cassandra.db.columniterator;
/*
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


import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.cache.CacheRowSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.CachedDataInput;
import org.apache.cassandra.io.util.FileDataInput;

public class CachedRowSliceIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private final ByteBuffer startColumn;
    private final ByteBuffer finishColumn;
    private final boolean reversed;
    private final CFMetaData metadata;

    private final DecoratedKey key;
    private final boolean noMergeNecessary;
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    private int currentIndex = Integer.MIN_VALUE;
    private ByteBuffer row;

    public CachedRowSliceIterator(CFMetaData metadata, FileDataInput input, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, int limit, boolean reversed, boolean noMergeNecessary)
    {
        this.key = key;
        this.noMergeNecessary = noMergeNecessary;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.reversed = reversed;
        this.metadata = metadata;

        this.row = ((CachedDataInput) input).getBuffer();

        try
        {
            read(row, limit);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private void read(ByteBuffer row, int limit) throws IOException
    {
        cf = ColumnFamily.create(metadata, ArrayBackedSortedColumns.factory());
        CacheRowSerializer.deserializeFromSSTableNoColumns(row, cf);
        if (limit <= 0 || noMergeNecessary)
        {
            CacheRowSerializer.appendRow(row, cf, startColumn, finishColumn, limit, reversed, metadata.comparator);
        }
    }

    private boolean isColumnNeeded(IColumn column)
    {
        if (startColumn.remaining() == 0 && finishColumn.remaining() == 0)
            return true;
        else if (startColumn.remaining() == 0 && !reversed)
            return metadata.comparator.compare(column.name(), finishColumn) <= 0;
        else if (startColumn.remaining() == 0 && reversed)
            return metadata.comparator.compare(column.name(), finishColumn) >= 0;
        else if (finishColumn.remaining() == 0 && !reversed)
            return metadata.comparator.compare(column.name(), startColumn) >= 0;
        else if (finishColumn.remaining() == 0 && reversed)
            return metadata.comparator.compare(column.name(), startColumn) <= 0;
        else if (!reversed)
            return metadata.comparator.compare(column.name(), startColumn) >= 0 && metadata.comparator.compare(column.name(), finishColumn) <= 0;
        else // if reversed
            return metadata.comparator.compare(column.name(), startColumn) <= 0 && metadata.comparator.compare(column.name(), finishColumn) >= 0;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    protected IColumn computeNext()
    {
        if (noMergeNecessary)
        {
            if (iter == null)
                iter = reversed ? cf.getReverseSortedColumns().iterator() : cf.iterator();
            
            if (!iter.hasNext())
                return endOfData();

            return iter.next();
        }
        else
        {
            if (currentIndex == Integer.MIN_VALUE) {
                if (startColumn.remaining() > 0) 
                {
                    int i = CacheRowSerializer.binarySearch(row, startColumn, metadata.comparator);
                    currentIndex = ((i < 0) ? reversed ? (-i - 2) : (-i - 1) : i);
                }
                else 
                    currentIndex = reversed ? CacheRowSerializer.getColumnCount(row) - 1 : 0;
                
            }
            int step;
            if (reversed)
            {
                if (currentIndex < 0)
                    return endOfData();

                step = -1;
            }
            else
            {
                if (currentIndex > CacheRowSerializer.getColumnCount(row) - 1)
                    return endOfData();

                step = 1;
            }

            Column column = CacheRowSerializer.createColumn(row, currentIndex);
            currentIndex += step;
            return isColumnNeeded(column) ? column : endOfData();
        }
    }
}
