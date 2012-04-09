package com.hmsonline.cassandra.index.dao;


public class DaoFactory {
    public static IndexDao getIndexDAO() {
        return new IndexDao();
    }

    public static ConfigurationDao getConfigurationDAO() {
        return new ConfigurationDao();
    }

    public static CommitLogDao getCommitLogDAO() {
        return new CommitLogDao();
    }
}
