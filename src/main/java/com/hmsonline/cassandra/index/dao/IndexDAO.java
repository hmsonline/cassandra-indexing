package com.hmsonline.cassandra.index.dao;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.ConsistencyLevel;

public interface IndexDAO {
  public void insertIndex(String indexName, ByteBuffer indexValue,
          ConsistencyLevel consistency);

  public void deleteIndex(String indexName, ByteBuffer indexValue,
          ConsistencyLevel consistency);
}
