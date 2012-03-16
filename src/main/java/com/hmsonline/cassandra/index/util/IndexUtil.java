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
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A helper class providing utility methods for handling indexing.
 * 
 * @author pnguyen
 */
public class IndexUtil {
  public static final String INDEXING_KEYSPACE = "Indexing";

  public static ByteBuffer buildIndex(Set<String> indexColumns, String rowKey,
          Map<String, String> currentRow) throws Exception {
    List<String> parts = new ArrayList<String>();
    for (String columnName : indexColumns) {
      parts.add(currentRow.get(columnName));
    }
    parts.add(rowKey);
    return CompositeUtil.compose(parts);
  }

  public static Map<String, String> getRowMutation(ColumnFamily columnFamily)
          throws Exception {
    Map<String, String> mutation = new HashMap<String, String>();
    for (ByteBuffer name : columnFamily.getColumnNames()) {
      IColumn column = columnFamily.getColumn(name);
      String columnName = ByteBufferUtil.string(name);
      mutation.put(columnName, column.isMarkedForDelete() ? null
              : ByteBufferUtil.string(column.value()));
    }
    return mutation;
  }

  public static Map<String, String> getNewRow(Map<String, String> currentRow,
          ColumnFamily columnFamily) throws Exception {
    Map<String, String> newRow = new HashMap<String, String>(currentRow);
    newRow.putAll(getRowMutation(columnFamily));
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

  public static boolean isEmptyRow(Map<String, String> row) {
    for (String value : row.values()) {
      if (value != null) {
        return false;
      }
    }
    return true;
  }

  public static Map<String, String> getRow(String keyspace,
          String columnFamily, String key) throws Exception {
    SliceRange slice = new SliceRange(ByteBufferUtil.bytes(""),
            ByteBufferUtil.bytes(""), false, 1000);
    SlicePredicate predicate = new SlicePredicate();
    predicate.setSlice_range(slice);

    ColumnParent parent = new ColumnParent(columnFamily);
    CassandraServer conn = new CassandraServer();
    conn.set_keyspace(keyspace);
    List<ColumnOrSuperColumn> columns = conn.get_slice(
            ByteBufferUtil.bytes(key), parent, predicate, ConsistencyLevel.ONE);
    Map<String, String> row = new HashMap<String, String>();

    for (ColumnOrSuperColumn column : columns) {
      row.put(ByteBufferUtil.string(column.column.name),
              ByteBufferUtil.string(column.column.value));
    }

    return row;
  }
}
