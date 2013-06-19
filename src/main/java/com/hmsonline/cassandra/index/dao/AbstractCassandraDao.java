package com.hmsonline.cassandra.index.dao;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.Cassandra;
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

public abstract class AbstractCassandraDao {
    private String keyspace;
    private String columnFamily;

    protected AbstractCassandraDao(String keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    protected List<ColumnOrSuperColumn> getSlice(ByteBuffer key, SlicePredicate predicate, ConsistencyLevel consistency)
            throws Exception {
        ColumnParent parent = new ColumnParent(columnFamily);
        return getConnection().get_slice(key, parent, predicate, consistency);
    }

    protected List<KeySlice> getRangeSlices(SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency)
            throws Exception {
        ColumnParent parent = new ColumnParent(columnFamily);
        return getConnection().get_range_slices(parent, predicate, range, consistency);
    }

    protected Cassandra.Iface insertColumn(ByteBuffer key, ByteBuffer columnName, ByteBuffer columnValue,
            ConsistencyLevel consistency, long timestamp) throws Exception {
        ColumnParent parent = new ColumnParent(columnFamily);
        Column column = createColumn(columnName, columnValue, timestamp);
        Cassandra.Iface cassandraInterface = getConnection();
        cassandraInterface.insert(key, parent, column, consistency);
        return cassandraInterface;
    }

    protected void deleteColumn(ByteBuffer key, ByteBuffer columnName, ConsistencyLevel consistency, long timestamp) throws Exception {
        ColumnPath path = new ColumnPath(columnFamily);
        path.setColumn(columnName);
        getConnection().remove(key, path, timestamp, consistency);
    }

    protected Iface getConnection() throws Exception {
        CassandraServer server = new CassandraServer();
        server.set_keyspace(keyspace);
        return server;
    }

    protected Column createColumn(ByteBuffer name, ByteBuffer value, long timestamp) {
        Column column = new Column();
        column.setName(name);
        column.setValue(value);
        column.setTimestamp(timestamp);
        return column;
    }

    protected Mutation createMutation(ByteBuffer name, ByteBuffer value, long timestamp) {
        ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn();
        superColumn.setColumn(createColumn(name, value, timestamp));
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(superColumn);
        return mutation;
    }
}
