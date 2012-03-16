package com.hmsonline.cassandra.index.dao;

import com.hmsonline.cassandra.index.dao.impl.ConfigurationDAOCassandra;
import com.hmsonline.cassandra.index.dao.impl.IndexDAOCassandra;

public class DAOFactory {
  public static IndexDAO getIndexDA0() {
    return new IndexDAOCassandra();
  }

  public static ConfigurationDAO getConfigurationDAO() {
    return new ConfigurationDAOCassandra();
  }
}
