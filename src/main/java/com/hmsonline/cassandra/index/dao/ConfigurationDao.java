package com.hmsonline.cassandra.index.dao;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.Configuration;
import com.hmsonline.cassandra.index.util.IndexUtil;

public class ConfigurationDao extends AbstractCassandraDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "Configuration";
    private static final int REFRESH_INTERVAL = 5 * 60 * 1000; // 15 Minutes

    private static Logger logger = LoggerFactory.getLogger(ConfigurationDao.class);
    private static long lastFetchTime = -1;
    private static Configuration config;

    public ConfigurationDao(Keyspace keyspace) {
        super(keyspace);
    }

    public Configuration getConfiguration() {
        long currentTime = System.currentTimeMillis();
        long timeSinceRefresh = currentTime - ConfigurationDao.lastFetchTime;

        if (config == null || config.isEmpty() || timeSinceRefresh > REFRESH_INTERVAL) {
            updateConfiguration();
        }
        return config;
    }
    
    public synchronized void updateConfiguration() {
        long currentTime = System.currentTimeMillis();
        long timeSinceRefresh = currentTime - ConfigurationDao.lastFetchTime;

        if (config == null || config.isEmpty() || timeSinceRefresh > REFRESH_INTERVAL) {
            logger.debug("Refreshing indexing configuration.");
            Configuration configuration = loadConfiguration();
            config = configuration;
        }
        ConfigurationDao.lastFetchTime = currentTime;        
    }

    private Configuration loadConfiguration() {
        try {
            Configuration config = new Configuration();

            RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory  
                    .createRangeSlicesQuery(this.getKeyspace(), StringSerializer.get(),  
                            StringSerializer.get(), StringSerializer.get());  
            rangeSlicesQuery.setColumnFamily(COLUMN_FAMILY);  
            rangeSlicesQuery.setKeys("", "");  
            rangeSlicesQuery.setRange("", "", false, 2000); // MAX_COLUMNS  
            rangeSlicesQuery.setRowCount(2000); // MAX_ROWS  
            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();  
            OrderedRows<String, String, String> orderedRows = result.get();  
            for (Row<String, String, String> r : orderedRows) {  
                ColumnSlice<String, String> slice = r.getColumnSlice();  
                String indexName = r.getKey();
                Map<String, String> indexProperties = new HashMap<String, String>();
                for (HColumn<String, String> column : slice.getColumns()) {  
                    if(logger.isDebugEnabled()) {
                        logger.debug("got " + COLUMN_FAMILY + "['" + r.getKey()  
                                        + "']" + "['" + column.getName() + "'] = '"  
                                        + column.getValue() + "';");
                    }
                    indexProperties.put(column.getName(), column.getValue());
                }  
                config.addIndex(indexName, indexProperties);
            }  
            return config;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load indexing configuration: " + KEYSPACE + ":" + COLUMN_FAMILY, ex);
        }
    }

    public static void forceRefresh() {
        ConfigurationDao.lastFetchTime = -1;
    }
}
