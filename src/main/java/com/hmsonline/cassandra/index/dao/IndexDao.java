package com.hmsonline.cassandra.index.dao;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.hmsonline.cassandra.index.util.IndexUtil;

public class IndexDao extends AbstractCassandraDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "Indexes";

    public IndexDao(Keyspace keyspace) {
        super(keyspace);
    }

    public void insertIndex(String indexName, String index, ConsistencyLevel consistency, long timestamp) {
        try {
            Mutator<String> mutator = HFactory.createMutator(this.getKeyspace(), StringSerializer.get());
            mutator.addInsertion(indexName, COLUMN_FAMILY,
                    HFactory.createColumn(index, "", timestamp));
            mutator.execute();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to insert index: " + indexName + "[" + index + "]", ex);
        }
    }

    public void insertIndexes(String indexName, List<String> indexes, ConsistencyLevel consistency, long timestamp) {
        for (String index : indexes) {
            insertIndex(indexName, index, consistency, timestamp);
        }
    }

    public void deleteIndex(String indexName, String index, ConsistencyLevel consistency, long timestamp) {
        try {
            Mutator<String> mutator = HFactory.createMutator(this.getKeyspace(), StringSerializer.get());
            mutator.addDeletion(indexName, COLUMN_FAMILY, index, StringSerializer.get(), timestamp);
            mutator.execute();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to delete index: " + indexName + "[" + index + "]", ex);
        }
    }

    public void deleteIndexes(String indexName, List<String> indexes, ConsistencyLevel consistency, long timestamp) {
        for (String index : indexes) {
            deleteIndex(indexName, index, consistency, timestamp);
        }
    }
}
