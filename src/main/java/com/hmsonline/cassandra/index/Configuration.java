package com.hmsonline.cassandra.index;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class Configuration {
  public static final String KEYSPACE = "keyspace";
  public static final String COLUMN_FAMILY = "column_family";
  public static final String COLUMNS = "columns";
  private static final String COLUMN_DELIM = ",";

  private Map<String, Map<String, Set<String>>> config = new HashMap<String, Map<String, Set<String>>>();

  public boolean isEmpty() {
    return config.isEmpty();
  }

  public void addIndex(String indexName, Map<String, String> indexProperties) {
    String keyspace = indexProperties.get(KEYSPACE);
    String columnFamily = indexProperties.get(COLUMN_FAMILY);
    String columns = indexProperties.get(COLUMNS);

    if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(columnFamily)
            || StringUtils.isEmpty(columns)) {
      return;
    }

    Set<String> columnSet = new LinkedHashSet<String>();
    for (String column : columns.split(COLUMN_DELIM)) {
      if (StringUtils.isNotEmpty(column)) {
        columnSet.add(column.trim());
      }
    }

    String key = generateKey(keyspace, columnFamily);
    if (!config.containsKey(key)) {
      config.put(key, new HashMap<String, Set<String>>());
    }
    config.get(key).put(indexName, columnSet);
  }

  public Map<String, Set<String>> getIndexes(String keyspace,
          String columnFamily) {
    String key = generateKey(keyspace, columnFamily);
    return config.containsKey(key) ? config.get(key)
            : new HashMap<String, Set<String>>();
  }

  public Set<String> getIndexNames(String keyspace, String columnFamily) {
    return getIndexes(keyspace, columnFamily).keySet();
  }

  public Set<String> getIndexColumns(String keyspace, String columnFamily,
          String indexName) {
    return getIndexes(keyspace, columnFamily).get(indexName);
  }

  private String generateKey(String keyspace, String columnFamily) {
    return keyspace.hashCode() + "_" + columnFamily.hashCode();
  }
}
