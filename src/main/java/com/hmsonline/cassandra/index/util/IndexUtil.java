package com.hmsonline.cassandra.index.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A helper class providing utility methods for handling indexing.
 * 
 * @author pnguyen
 */
public class IndexUtil {
  public static final String INDEXING_KEYSPACE = "Indexing";

  public static ByteBuffer buildIndex(Set<String> indexColumns, String rowKey,
          Map<String, String> row) throws Exception {
    List<String> parts = new ArrayList<String>();
    for (String columnName : indexColumns) {
      parts.add(row.get(columnName));
    }
    parts.add(rowKey);
    return CompositeUtil.compose(parts);
  }

  public static Map<String, String> getRowMutation(ColumnFamily columnFamily,
          Set<String> indexColumns) throws Exception {
    Map<String, String> mutation = new HashMap<String, String>();
    for (ByteBuffer name : columnFamily.getColumnNames()) {
      String columnName = ByteBufferUtil.string(name);
      if (!indexColumns.contains(columnName)) {
        continue;
      }
      IColumn column = columnFamily.getColumn(name);
      mutation.put(columnName, column.isMarkedForDelete() ? null
              : ByteBufferUtil.string(column.value()));
    }
    return mutation;
  }

  public static Map<String, String> getNewRow(Map<String, String> currentRow,
          ColumnFamily columnFamily, Set<String> indexColumns) throws Exception {
    Map<String, String> newRow = new HashMap<String, String>(currentRow);
    newRow.putAll(getRowMutation(columnFamily, indexColumns));
    return newRow;
  }

  public static boolean indexChanged(Set<String> indexColumns,
          ColumnFamily columnFamily) throws Exception {
    for (ByteBuffer columnName : columnFamily.getColumnNames()) {
      if (indexColumns.contains(ByteBufferUtil.string(columnName))) {
        return true;
      }
    }
    return false;
  }

  public static boolean isEmptyIndex(Set<String> indexColumns,
          Map<String, String> row) {
    for (String column : indexColumns) {
      if (row.get(column) != null) {
        return false;
      }
    }
    return true;
  }

  public static Map<String, String> fetchRow(String keyspace,
          String columnFamily, String key, Set<String> indexColumns)
          throws Exception {
    List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
    for (String column : indexColumns) {
      columnNames.add(ByteBufferUtil.bytes(column));
    }

    SlicePredicate predicate = new SlicePredicate();
    predicate.setColumn_names(columnNames);
    ColumnParent parent = new ColumnParent(columnFamily);
    CassandraServer conn = new CassandraServer();
    conn.set_keyspace(keyspace);
    List<ColumnOrSuperColumn> columns = conn.get_slice(
            ByteBufferUtil.bytes(key), parent, predicate, ConsistencyLevel.ONE);
    Map<String, String> result = new HashMap<String, String>();

    for (ColumnOrSuperColumn column : columns) {
      result.put(ByteBufferUtil.string(column.column.name),
              ByteBufferUtil.string(column.column.value));
    }

    return result;
  }
}
