package com.hmsonline.cassandra.index.dao;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.hmsonline.cassandra.index.util.IndexUtil;

public class IndexDao extends AbstractCassandraDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "Indexes";

    public IndexDao() {
        super(KEYSPACE, COLUMN_FAMILY);
    }

    public void insertIndex(String indexName, ByteBuffer indexValue, ConsistencyLevel consistency) {
        try {
            insertColumn(ByteBufferUtil.bytes(indexName), indexValue, ByteBufferUtil.EMPTY_BYTE_BUFFER, consistency);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to insert index: " + indexName + "[" + indexValue + "]", ex);
        }
    }

    public void deleteIndex(String indexName, ByteBuffer indexValue, ConsistencyLevel consistency) {
        try {
            deleteColumn(ByteBufferUtil.bytes(indexName), indexValue, consistency);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to delete index: " + indexName + "[" + indexValue + "]", ex);
        }
    }
}
