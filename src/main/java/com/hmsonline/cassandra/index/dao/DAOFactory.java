package com.hmsonline.cassandra.index.dao;

import com.hmsonline.cassandra.index.dao.impl.CommitLogDaoCassandra;
import com.hmsonline.cassandra.index.dao.impl.ConfigurationDaoCassandra;
import com.hmsonline.cassandra.index.dao.impl.IndexDaoCassandra;

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
