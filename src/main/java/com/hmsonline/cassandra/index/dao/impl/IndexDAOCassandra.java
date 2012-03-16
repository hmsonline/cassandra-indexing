package com.hmsonline.cassandra.index.dao.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.hmsonline.cassandra.index.dao.IndexDAO;
import com.hmsonline.cassandra.index.util.CompositeUtil;

public class IndexDAOCassandra extends AbstractCassandraDAO implements IndexDAO {
  private static final String KEYSPACE = "Indexing";
  private static final String COLUMN_FAMILY = "Indexes";

  public IndexDAOCassandra() {
    super(KEYSPACE, COLUMN_FAMILY);
  }

  public ByteBuffer findIndex(String sourceKey, String indexName, int indexNum) {
    try {
      String[] parts = new String[indexNum + 1];
      parts[indexNum] = sourceKey;
      ByteBuffer start = CompositeUtil.compose(Arrays.asList(parts), false);
      parts[indexNum] = sourceKey + Character.MAX_VALUE;
      ByteBuffer finish = CompositeUtil.compose(Arrays.asList(parts), false);

      SliceRange slice = new SliceRange(start, finish, false, 1);
      SlicePredicate predicate = new SlicePredicate();
      predicate.setSlice_range(slice);

      List<ColumnOrSuperColumn> columns = getSlice(
              ByteBufferUtil.bytes(indexName), predicate, ConsistencyLevel.ALL);
      return columns.isEmpty() ? null : columns.get(0).column.name;
    }
    catch (Exception ex) {
      throw new RuntimeException("Failed to find index: " + indexName + "["
              + sourceKey + "]", ex);
    }
  }

  public void insertIndex(String indexName, ByteBuffer indexValue,
          ConsistencyLevel consistency) {
    try {
      insertColumn(ByteBufferUtil.bytes(indexName), indexValue,
              ByteBuffer.wrap(new byte[0]), consistency);
    }
    catch (Exception ex) {
      throw new RuntimeException("Failed to insert index: " + indexName + "["
              + indexValue + "]", ex);
    }
  }

  public void deleteIndex(String indexName, ByteBuffer column,
          ConsistencyLevel consistency) {
    try {
      deleteColumn(ByteBufferUtil.bytes(indexName), column, consistency);
    }
    catch (Exception ex) {
      throw new RuntimeException("Failed to delete index: " + indexName + "["
              + column + "]", ex);
    }
  }

  public void deleteIndex(String sourceKey, String indexName, int indexNum,
          ConsistencyLevel consistency) {
    ByteBuffer column = findIndex(sourceKey, indexName, indexNum);
    if (column != null) {
      deleteIndex(indexName, column, consistency);
    }
  }
}
