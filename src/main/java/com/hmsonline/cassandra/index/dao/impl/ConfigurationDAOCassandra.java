//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.cassandra.index.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.Configuration;
import com.hmsonline.cassandra.index.dao.ConfigurationDAO;

public class ConfigurationDAOCassandra extends AbstractCassandraDAO implements
        ConfigurationDAO {

  private static Logger logger = LoggerFactory
          .getLogger(ConfigurationDAOCassandra.class);
  private static final String KEYSPACE = "Indexing";
  private static final String COLUMN_FAMILY = "Configuration";
  private static final int REFRESH_INTERVAL = 30 * 1000; // 30 seconds

  private static long lastFetchTime = -1;
  private static Configuration config;

  public ConfigurationDAOCassandra() {
    super(KEYSPACE, COLUMN_FAMILY);
  }

  public Configuration getConfiguration() {
    long currentTime = System.currentTimeMillis();
    long timeSinceRefresh = currentTime - this.lastFetchTime;

    if (config == null || timeSinceRefresh > REFRESH_INTERVAL) {
      logger.debug("Refreshing indexing configuration.");
      Configuration configuration = loadConfiguration();

      if (config == null) {
        config = configuration;
      }
      else {
        synchronized (config) {
          config = configuration;
        }
      }
      this.lastFetchTime = currentTime;
    }

    return config;
  }

  private Configuration loadConfiguration() {
    try {
      Configuration config = new Configuration();

      SliceRange range = new SliceRange(ByteBufferUtil.bytes(""),
              ByteBufferUtil.bytes(""), false, 10);
      SlicePredicate predicate = new SlicePredicate();
      predicate.setSlice_range(range);

      KeyRange keyRange = new KeyRange(Integer.MAX_VALUE);
      keyRange.setStart_key(ByteBufferUtil.bytes(""));
      keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
      List<KeySlice> slices = getRangeSlices(predicate, keyRange,
              ConsistencyLevel.ALL);

      for (KeySlice slice : slices) {
        String indexName = ByteBufferUtil.string(slice.key);
        Map<String, String> indexProperties = new HashMap<String, String>();
        for (ColumnOrSuperColumn column : slice.columns) {
          indexProperties.put(ByteBufferUtil.string(column.column.name),
                  ByteBufferUtil.string(column.column.value));
        }
        config.addIndex(indexName, indexProperties);
      }

      return config;
    }
    catch (Exception ex) {
      throw new RuntimeException("Failed to load indexing configuration: "
              + KEYSPACE + ":" + COLUMN_FAMILY, ex);
    }
  }
}
