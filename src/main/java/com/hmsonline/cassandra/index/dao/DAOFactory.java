package com.hmsonline.cassandra.index.dao;


public class DaoFactory {
    public static IndexDao getIndexDAO() {
        return new IndexDaoCassandra();
    }

    public static ConfigurationDao getConfigurationDAO() {
        return new ConfigurationDaoCassandra();
    }

    public static CommitLogDao getCommitLogDAO() {
        return new CommitLogDaoCassandra();
    }
}
