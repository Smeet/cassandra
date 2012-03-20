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
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CacheRowSerializer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.util.CachedDataInput;
import org.apache.cassandra.io.util.FileDataInput;

public class CachedRowNamesIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private static Logger logger = LoggerFactory.getLogger(CachedRowNamesIterator.class);

    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final DecoratedKey key;

    public CachedRowNamesIterator(CFMetaData metadata, FileDataInput file, DecoratedKey key, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.key = key;

        try                                                                    
        {
            this.cf = ColumnFamily.create(metadata, ArrayBackedSortedColumns.factory());
            read(((CachedDataInput)file).getBuffer(), metadata, columns);
        }
        catch (IOException ioe)
        {
            throw new IOError(ioe);
        }
    }


    private void read(ByteBuffer row, CFMetaData metadata, SortedSet<ByteBuffer> columns)
            throws IOException
    {
        CacheRowSerializer.deserializeFromSSTableNoColumns(row, cf);
        for (ByteBuffer column : columns) {
            IColumn col = CacheRowSerializer.deserializeColumn(row, column, metadata.comparator);
            if (col != null) {
                // we are adding in sort order so this should be cheap
                cf.addColumn(col);
            }
        }        
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected IColumn computeNext()
    {
        if (iter == null) {
            iter = cf.iterator();
        }
        if (!iter.hasNext())
            return endOfData();
        
        return iter.next();
    }
}