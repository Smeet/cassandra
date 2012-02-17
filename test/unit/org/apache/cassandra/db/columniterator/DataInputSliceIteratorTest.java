package org.apache.cassandra.db.columniterator;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.CachedDataInput;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * @author smeet
 */
public class DataInputSliceIteratorTest
{
    public static final CFMetaData CF_META_DATA = new CFMetaData("Test", "Test", ColumnFamilyType.Standard, BytesType.instance, null);

    @Test
    public void testSlicing()
    {
        DataInputSliceIterator iter = new DataInputSliceIterator(CF_META_DATA, getRow(), null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, false);
        for (int i = 0; i < 10; i++)
        {
            IColumn next = iter.next();
            Assert.assertEquals(bytes(i * 2 + 2), next.name());
        }
        iter = new DataInputSliceIterator(CF_META_DATA, getRow(), null, EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER, true);
        for (int i = 9; i>=0; i--)
        {
            IColumn next = iter.next();
            Assert.assertEquals(bytes(i * 2 + 2), next.name());            
        }
    }

    private FileDataInput getRow()
    {
        return new CachedDataInput(CachedDataInput.serialize(getColumnFamily()));
    }

    private ColumnFamily getColumnFamily()
    {
        ColumnFamily columns = ColumnFamily.create(CF_META_DATA);
        for (int i = 0; i < 10; i++)
            columns.addColumn(QueryPath.column(bytes(i * 2 + 2)), bytes(0), timestamp());
        return columns;
    }

    private long timestamp()
    {
        return System.nanoTime() / 1000;
    }

    
}
