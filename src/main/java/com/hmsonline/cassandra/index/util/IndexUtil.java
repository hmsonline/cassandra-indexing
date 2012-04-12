package com.hmsonline.cassandra.index.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.hmsonline.cassandra.index.LogEntry;
import com.hmsonline.cassandra.index.LogEntry.Status;

/**
 * A helper class providing utility methods for handling indexing.
 * 
 * @author pnguyen
 */
public class IndexUtil {
    public static final String INDEXING_KEYSPACE = "Indexing";

    public static ByteBuffer buildIndex(Set<String> indexColumns, String rowKey, Map<String, String> row)
            throws Exception {
        List<String> parts = new ArrayList<String>();
        for (String columnName : indexColumns) {
            parts.add(row.get(columnName));
        }
        parts.add(rowKey);
        return CompositeUtil.compose(parts);
    }

    public static Map<String, String> getRowMutation(ColumnFamily columnFamily) throws Exception {
        Map<String, String> mutation = new HashMap<String, String>();
        for (IColumn column : columnFamily.getSortedColumns()) {
            String value = column.isMarkedForDelete() ? null : ByteBufferUtil.string(column.value());
            mutation.put(ByteBufferUtil.string(column.name()), value);
        }
        return mutation;
    }

    public static Map<String, String> getRowMutation(ColumnFamily columnFamily, Set<String> indexColumns)
            throws Exception {
        Map<String, String> mutation = new HashMap<String, String>();
        for (IColumn column : columnFamily.getSortedColumns()) {
            String columnName = ByteBufferUtil.string(column.name());
            if (!indexColumns.contains(columnName)) {
                continue;
            }
            mutation.put(columnName, column.isMarkedForDelete() ? null : ByteBufferUtil.string(column.value()));
        }
        return mutation;
    }

    public static Map<String, String> getNewRow(Map<String, String> currentRow, ColumnFamily columnFamily,
            Set<String> indexColumns) throws Exception {
        Map<String, String> newRow = new HashMap<String, String>(currentRow);
        newRow.putAll(getRowMutation(columnFamily, indexColumns));
        return newRow;
    }

    public static boolean indexChanged(Set<String> indexColumns, ColumnFamily columnFamily) throws Exception {
        for (ByteBuffer columnName : columnFamily.getColumnNames()) {
            if (indexColumns.contains(ByteBufferUtil.string(columnName))) {
                return true;
            }
        }
        return false;
    }

    public static boolean isEmptyIndex(Set<String> indexColumns, Map<String, String> row) {
        for (String column : indexColumns) {
            if (row.get(column) != null) {
                return false;
            }
        }
        return true;
    }

    public static Map<String, String> fetchRow(String keyspace, String columnFamily, String key,
            Set<String> indexColumns) throws Exception {
        List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
        for (String column : indexColumns) {
            columnNames.add(ByteBufferUtil.bytes(column));
        }

        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(columnNames);
        ColumnParent parent = new ColumnParent(columnFamily);
        CassandraServer conn = new CassandraServer();
        conn.set_keyspace(keyspace);
        List<ColumnOrSuperColumn> columns = conn.get_slice(ByteBufferUtil.bytes(key), parent, predicate,
                ConsistencyLevel.ONE);
        Map<String, String> result = new HashMap<String, String>();

        for (ColumnOrSuperColumn column : columns) {
            result.put(ByteBufferUtil.string(column.column.name), ByteBufferUtil.string(column.column.value));
        }
        return result;
    }

    public static List<LogEntry> getLogEntries(List<IMutation> mutations) throws Exception {
        List<LogEntry> entries = new ArrayList<LogEntry>();

        for (IMutation mutation : mutations) {
            String keyspace = mutation.getTable();
            if (INDEXING_KEYSPACE.equals(keyspace)) {
                continue;
            }

            for (ColumnFamily cf : ((RowMutation) mutation).getColumnFamilies()) {
                entries.add(new LogEntry(keyspace, cf.metadata().cfName, ByteBufferUtil.string(mutation.key()),
                        Status.PENDING, getRowMutation(cf)));
            }
        }
        return entries;
    }
}
