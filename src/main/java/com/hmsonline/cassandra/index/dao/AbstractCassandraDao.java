package com.hmsonline.cassandra.index.dao;

import me.prettyprint.hector.api.Keyspace;

public abstract class AbstractCassandraDao {
    private Keyspace keyspace;

    protected AbstractCassandraDao(Keyspace keyspace) {
        this.keyspace = keyspace;
    }
    public Keyspace getKeyspace() throws Exception {
        return this.keyspace;
    }
}
