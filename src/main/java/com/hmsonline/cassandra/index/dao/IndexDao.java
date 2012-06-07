package com.hmsonline.cassandra.index.dao;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.hmsonline.cassandra.index.util.IndexUtil;

public class IndexDao extends AbstractCassandraDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "Indexes";

    public IndexDao() {
        super(KEYSPACE, COLUMN_FAMILY);
    }

    public void insertIndex(String indexName, ByteBuffer index, ConsistencyLevel consistency, long timestamp) {
        try {
            insertColumn(ByteBufferUtil.bytes(indexName), index, ByteBufferUtil.EMPTY_BYTE_BUFFER, consistency, timestamp);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to insert index: " + indexName + "[" + index + "]", ex);
        }
    }

    public void insertIndexes(String indexName, List<ByteBuffer> indexes, ConsistencyLevel consistency, long timestamp) {
        for (ByteBuffer index : indexes) {
            insertIndex(indexName, index, consistency, timestamp);
        }
    }

    public void deleteIndex(String indexName, ByteBuffer index, ConsistencyLevel consistency, long timestamp) {
        try {
            deleteColumn(ByteBufferUtil.bytes(indexName), index, consistency, timestamp);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to delete index: " + indexName + "[" + index + "]", ex);
        }
    }

    public void deleteIndexes(String indexName, List<ByteBuffer> indexes, ConsistencyLevel consistency, long timestamp) {
        for (ByteBuffer index : indexes) {
            deleteIndex(indexName, index, consistency, timestamp);
        }
    }
}
