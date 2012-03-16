//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.cassandra.index.dao;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.ConsistencyLevel;

public interface IndexDAO {
  public ByteBuffer findIndex(String sourceKey, String indexName, int indexNum);

  public void insertIndex(String indexName, ByteBuffer indexValue,
          ConsistencyLevel consistency);

  public void deleteIndex(String indexName, ByteBuffer indexValue,
          ConsistencyLevel consistency);

  public void deleteIndex(String sourceKey, String indexName, int indexNum,
          ConsistencyLevel consistency);
}
