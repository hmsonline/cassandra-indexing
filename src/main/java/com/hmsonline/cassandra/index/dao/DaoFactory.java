package com.hmsonline.cassandra.index.dao;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;


public class DaoFactory {
    public static IndexDao getIndexDAO(Cluster cluster) {
        Keyspace keyspace = HFactory.createKeyspace(IndexDao.KEYSPACE, cluster);
        return new IndexDao(keyspace);
    }

    public static ConfigurationDao getConfigurationDAO(Cluster cluster) {
        Keyspace keyspace = HFactory.createKeyspace(ConfigurationDao.KEYSPACE, cluster);
        return new ConfigurationDao(keyspace);
    }
}
