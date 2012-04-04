package com.hmsonline.cassandra.index.dao.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

public abstract class AbstractCassandraDAO {
  private String keyspace;
  private String columnFamily;

  protected AbstractCassandraDAO(String keyspace, String columnFamily) {
    this.keyspace = keyspace;
    this.columnFamily = columnFamily;
  }

  protected List<ColumnOrSuperColumn> getSlice(ByteBuffer key,
          SlicePredicate predicate, ConsistencyLevel consistency)
          throws Exception {
    ColumnParent parent = new ColumnParent(columnFamily);
    return getConnection().get_slice(key, parent, predicate, consistency);
  }

  protected List<KeySlice> getRangeSlices(SlicePredicate predicate,
          KeyRange range, ConsistencyLevel consistency) throws Exception {
    ColumnParent parent = new ColumnParent(columnFamily);
    return getConnection().get_range_slices(parent, predicate, range,
            consistency);
  }

  protected void insertColumn(ByteBuffer key, ByteBuffer columnName,
          ByteBuffer columnValue, ConsistencyLevel consistency)
          throws Exception {
    ColumnParent parent = new ColumnParent(columnFamily);
    Column column = createColumn(columnName, columnValue);
    getConnection().insert(key, parent, column, consistency);
  }

  protected void deleteColumn(ByteBuffer key, ByteBuffer columnName,
          ConsistencyLevel consistency) throws Exception {
    ColumnPath path = new ColumnPath(columnFamily);
    path.setColumn(columnName);
    getConnection().remove(key, path, getTimestamp(), consistency);
  }

  protected Iface getConnection() throws Exception {
    CassandraServer server = new CassandraServer();
    server.set_keyspace(keyspace);
    return server;
  }

  protected Column createColumn(ByteBuffer name, ByteBuffer value) {
    Column column = new Column();
    column.setName(name);
    column.setValue(value);
    column.setTimestamp(getTimestamp() + 1);
    return column;
  }

  protected Mutation createMutation(ByteBuffer name, ByteBuffer value) {
    ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn();
    superColumn.setColumn(createColumn(name, value));
    Mutation mutation = new Mutation();
    mutation.setColumn_or_supercolumn(superColumn);
    return mutation;
  }

  protected long getTimestamp() {
    return System.nanoTime() * 10;
  }
}
