package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.DBConstants.*;
import static org.apache.cassandra.io.IColumnSerializer.Flag.LOCAL;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Format:
 * <p/>
 * ===========================
 * Header
 * ===========================
 * MaxTimestamp:        long
 * LocalDeletionTime:   int
 * MarkedForDeleteAt:   long
 * NumColumns:          int
 * ===========================
 * Column Index
 * ===========================
 * NameOffset:          int
 * ValueOffset:         int
 * ValueLength:         int
 * ===========================
 * Column Data
 * ===========================
 * Name:                byte[]
 * Value:               byte[]
 * SerializationFlags:  byte
 * Misc:                ?
 * Timestamp:           long
 * ---------------------------
 * Misc Counter Column
 * ---------------------------
 * TSOfLastDelete:      long
 * ---------------------------
 * Misc Expiring Column
 * ---------------------------
 * TimeToLive:          int
 * LocalDeletionTime:   int
 * ===========================
 *
 * @author smeet
 */
public class CacheRowSerializer
{

    private static final Logger logger = LoggerFactory.getLogger(CacheRowSerializer.class);

    public static final int headerSize = longSize // max ts                
            + intSize   // local deletion
            + longSize  // marked for delete
            + intSize;  // num columns

    public static final int colIndexSize = intSize // name offset
            + intSize   // value offsets
            + intSize;  // value length

    public static final int maxTimestampPos = 0;
    public static final int localDeletionPos = longSize;
    public static final int markedForDeleteAtPos = localDeletionPos + intSize;
    public static final int numColumnsPos = markedForDeleteAtPos + longSize;

    public static ByteBuffer serialize(ColumnFamily cf)
    {
        Collection<IColumn> sortedColumns = cf.getSortedColumns();

        // this might be expensive
        int numColumns = sortedColumns.size();

        int serializedSize = getSerializedSize(sortedColumns);
        ByteBuffer row = ByteBuffer.allocate(serializedSize);

        serializeHeader(cf, numColumns, row);

        int dataOffset = headerSize + numColumns * colIndexSize;

        serializeColumns(sortedColumns, dataOffset, row);

        return row;
    }

    private static int getSerializedSize(Collection<IColumn> columns)
    {
        int size = headerSize;

        for (IColumn column : columns)
        {
            size += column.size()
                    + shortSize // instead of short name lenght we store a int offset
                    + intSize;  // name offset                    
        }

        return size;
    }

    private static void serializeColumns(Collection<IColumn> sortedColumns, int dataBaseOffset, ByteBuffer row)
    {
        int dataOffset = dataBaseOffset;
        int indexOffset = headerSize;
        for (IColumn column : sortedColumns)
        {
            ByteBuffer name = column.name();
            int nameLength = name.remaining();
            ByteBuffer value = column.value();
            int valueLength = value.remaining();

            // write index entry
            row.position(indexOffset);


//            logger.info("Put dataoffset {} @ {}", dataOffset, row.position());
            row.putInt(dataOffset);
            row.putInt(dataOffset + nameLength);
            row.putInt(valueLength);

            indexOffset = row.position();

            // write data
            ByteBufferUtil.arrayCopy(name, name.position(), row, dataOffset, nameLength);
            dataOffset += nameLength;
            ByteBufferUtil.arrayCopy(value, value.position(), row, dataOffset, valueLength);
            dataOffset += valueLength;

            row.position(dataOffset);
            row.put((byte) column.serializationFlags());
            if (column instanceof CounterColumn)
            {
                row.putLong(((CounterColumn) column).timestampOfLastDelete());
            }
            else if (column instanceof ExpiringColumn)
            {
                row.putInt(((ExpiringColumn) column).getTimeToLive());
                row.putInt(column.getLocalDeletionTime());
            }
            row.putLong(column.timestamp());
            dataOffset = row.position();
        }

        if (row.remaining() > 0)
        {
            throw new IllegalStateException("Invalid row size!");
        }
    }

    private static void serializeHeader(ColumnFamily cf, int numColumns, ByteBuffer row)
    {
        row.putLong(cf.maxTimestamp());
        row.putInt(cf.getLocalDeletionTime());
        row.putLong(cf.getMarkedForDeleteAt());
        row.putInt(numColumns);
    }

    public static int getColumnCount(ByteBuffer row)
    {
        return row.getInt(numColumnsPos);
    }

    public static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        int lastColumnIndex = row.getInt(numColumnsPos) - 1;
        return binarySearch(row, name, comparator, 0, lastColumnIndex);
    }

    private static int binarySearch(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator, int low, int high)
    {
        ByteBuffer midKey = row.duplicate();
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int indexOffset = headerSize + mid * colIndexSize;
            int nameOffset = row.getInt(indexOffset);
            int valueOffset = row.getInt(indexOffset + intSize);

            midKey.limit(valueOffset);
            midKey.position(nameOffset);

            int compare = comparator.compare(midKey, name);
            if (compare < 0)
            {
                low = mid + 1;

            }
            else if (compare > 0)
            {
                high = mid - 1;

            }
            else
            {
                return mid;
            }
        }
        return -(low + 1);
    }

    public static IColumn deserializeColumn(ByteBuffer row, ByteBuffer name, Comparator<ByteBuffer> comparator)
    {
        int low = 0;
        row = row.duplicate();
        int high = row.getInt(numColumnsPos) - 1;

        ByteBuffer midKey = row.duplicate();
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int indexOffset = headerSize + mid * colIndexSize;
            int nameOffset = row.getInt(indexOffset);
            int valueOffset = row.getInt(indexOffset + intSize);

            midKey.limit(valueOffset);
            midKey.position(nameOffset);

            int compare = comparator.compare(midKey, name);
            if (compare < 0)
            {
                low = mid + 1;
            }
            else if (compare > 0)
            {
                high = mid - 1;
            }
            else
            {
                return createColumn(row, midKey, valueOffset, row.getInt(indexOffset + longSize));
            }
        }

        return null;
    }

    public static void appendRow(ByteBuffer row, ColumnFamily cf, ByteBuffer startColumn, ByteBuffer finishColumn, 
                                 int limit, boolean reversed, Comparator<ByteBuffer> comparator)
    {
        int numColumns = row.getInt(numColumnsPos);

        if (numColumns == 0)
            return;

        row = row.duplicate();
        int startIndex, endIndex;
        int step = reversed ? -1 : 1;      
        if (startColumn.remaining() == 0 && finishColumn.remaining() == 0) 
        {
            if (reversed)
            {
                startIndex = numColumns - 1;
                endIndex = ((limit > 0) ? Math.max(startIndex - limit + 1, 0) : 0);                
            }
            else 
            {
                startIndex = 0;
                endIndex = ((limit > 0) ? Math.min(limit, numColumns) : numColumns) - 1;
            }
        }
        else if (startColumn.remaining() == 0)
        {
            int i = binarySearch(row, finishColumn, comparator);
            if (reversed)
            {
                startIndex = numColumns - 1;
                endIndex = i < 0 ? (-i - 1) : i;
                if (limit > 0)
                    endIndex = Math.max(startIndex - limit + 1, endIndex); 
            }
            else 
            {
                startIndex = 0;
                endIndex = i < 0 ? (-i - 2) : i;
                if (limit > 0)
                    endIndex = Math.min(limit - 1, endIndex); 
            }
        }
        else if (finishColumn.remaining() == 0)
        {
            int i = binarySearch(row, startColumn, comparator);
            if (reversed)
            {
                startIndex = i < 0 ? (-i - 2) : i;
                endIndex = ((limit > 0) ? Math.max(startIndex - limit + 1, 0) : 0);
            }
            else 
            {
                startIndex = i < 0 ? (-i - 1) : i;
                endIndex = ((limit > 0) ? Math.min(startIndex + limit, numColumns) : numColumns) - 1;
            }
        }
        else 
        {
            int i = binarySearch(row, startColumn, comparator);
            if (reversed)
            {
                startIndex = i < 0 ? (-i - 2) : i;
                int j = binarySearch(row, finishColumn, comparator, 0, startIndex);
                endIndex = j < 0 ? (-j - 1) : j;
                if (limit > 0)
                    endIndex = Math.max(startIndex - limit + 1, endIndex);
            }
            else 
            {
                startIndex = i < 0 ? (-i - 1) : i;
                int j = binarySearch(row, finishColumn, comparator, startIndex, numColumns - 1);
                endIndex = j < 0 ? (-j - 2) : j;
                if (limit > 0)
                    endIndex = Math.min(startIndex + limit - 1, endIndex);
            }
        }

        if (reversed && startIndex < endIndex || 
            !reversed && endIndex < startIndex )
            return;

        while (true)
        {
            Column column = createColumn(row, startIndex);
            cf.addColumn(column);

            if (startIndex == endIndex)
                return;
            
            startIndex += step;           
        }
    }

    public static Column createColumn(ByteBuffer row, int columnIndex)
    {
        int indexOffset = headerSize + columnIndex * 3 * intSize;
        int nameOffset = row.getInt(indexOffset);
        indexOffset += intSize;
        int valueOffset = row.getInt(indexOffset);
        indexOffset += intSize;
        int valueLength = row.getInt(indexOffset);
        indexOffset += intSize;
        return createColumn(row, nameOffset, valueOffset, valueLength);
    }

    private static Column createColumn(ByteBuffer row, int nameOffset, int valueOffset, int valueLength)
    {
        ByteBuffer name = row.duplicate();
        name.position(nameOffset);
        name.limit(valueOffset);
        return createColumn(row, name, valueOffset, valueLength);
    }

    private static Column createColumn(ByteBuffer row, ByteBuffer name, int valueOffset, int valueLength)
    {
        int flagsOffset = valueOffset + valueLength;
        ByteBuffer value = row.duplicate();
        value.position(valueOffset);
        value.limit(flagsOffset);

        int oldPosition = row.position();
        try
        {
            row.position(flagsOffset);
            byte serializationFlags = row.get();
            if ((serializationFlags & ColumnSerializer.COUNTER_MASK) != 0)
            {
                long timestampOfLastDelete = row.getLong();
                long ts = row.getLong();
                return (CounterColumn.create(name, value, ts, timestampOfLastDelete, LOCAL));
            }
            else if ((serializationFlags & ColumnSerializer.EXPIRATION_MASK) != 0)
            {
                int ttl = row.getInt();
                int expiration = row.getInt();
                long ts = row.getLong();
                int expireBefore = (int) (System.currentTimeMillis() / 1000);
                return (ExpiringColumn.create(name, value, ts, ttl, expiration, expireBefore, LOCAL));
            }
            else
            {
                long ts = row.getLong();
                return ((serializationFlags & ColumnSerializer.COUNTER_UPDATE_MASK) != 0
                        ? new CounterUpdateColumn(name, value, ts)
                        : ((serializationFlags & ColumnSerializer.DELETION_MASK) == 0
                        ? new Column(name, value, ts)
                        : new DeletedColumn(name, value, ts)));
            }
        }
        finally
        {
            row.position(oldPosition);
        }
    }

    public static void deserializeFromSSTableNoColumns(ByteBuffer row, ColumnFamily cf)
    {
        cf.delete(row.getInt(localDeletionPos), row.getLong(markedForDeleteAtPos));
    }

}
