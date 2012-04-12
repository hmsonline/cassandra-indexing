package com.hmsonline.cassandra.index.dao;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.Configuration;
import com.hmsonline.cassandra.index.util.IndexUtil;

public class ConfigurationDao extends AbstractCassandraDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "Configuration";
    private static final int REFRESH_INTERVAL = 30 * 1000; // 30 seconds

    private static Logger logger = LoggerFactory.getLogger(ConfigurationDao.class);
    private static long lastFetchTime = -1;
    private static Configuration config;

    public ConfigurationDao() {
        super(KEYSPACE, COLUMN_FAMILY);
    }

    public Configuration getConfiguration() {
        long currentTime = System.currentTimeMillis();
        long timeSinceRefresh = currentTime - ConfigurationDao.lastFetchTime;

        if (config == null || config.isEmpty() || timeSinceRefresh > REFRESH_INTERVAL) {
            logger.debug("Refreshing indexing configuration.");
            Configuration configuration = loadConfiguration();

            if (config == null) {
                config = configuration;
            } else {
                synchronized (config) {
                    config = configuration;
                }
            }
            ConfigurationDao.lastFetchTime = currentTime;
        }
        return config;
    }

    private Configuration loadConfiguration() {
        try {
            Configuration config = new Configuration();

            SlicePredicate predicate = new SlicePredicate();
            predicate.setColumn_names(Arrays.asList(ByteBufferUtil.bytes(Configuration.KEYSPACE),
                    ByteBufferUtil.bytes(Configuration.COLUMN_FAMILY), ByteBufferUtil.bytes(Configuration.COLUMNS)));

            KeyRange keyRange = new KeyRange(1000);
            keyRange.setStart_key(ByteBufferUtil.bytes(""));
            keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            List<KeySlice> slices = getRangeSlices(predicate, keyRange, ConsistencyLevel.ONE);

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
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load indexing configuration: " + KEYSPACE + ":" + COLUMN_FAMILY, ex);
        }
    }
}
