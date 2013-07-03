package com.hmsonline.cassandra.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.hmsonline.cassandra.index.dao.ConfigurationDao;
import com.hmsonline.cassandra.index.dao.DaoFactory;
import com.hmsonline.cassandra.index.dao.IndexDao;


public abstract class AbstractIndexingTest {
    protected static final String CLUSTER_NAME = "Test Cluster";
    protected static final String CASSANDRA_HOST = "localhost";
    protected static final int CASSANDRA_PORT = 9160;

    private static final String INDEX_KS = IndexDao.KEYSPACE;
    protected static final String INDEX_CF = IndexDao.COLUMN_FAMILY;
    protected static final String CONF_CF = ConfigurationDao.COLUMN_FAMILY;
    private static final String DATA_KS = "ks";
    protected static final String DATA_CF = "cf";
    protected static final String DATA_CF2 = "cf2";
    protected static final String INDEX_NAME = "test_idx";
    protected static final String INDEX_NAME2 = "test2_idx";
    protected static final String INDEX_NAME3 = "test3_idx";
    protected static final String MULTI_VALUE_INDEX_NAME = "test4_idx";
    protected static final String MULTI_VALUE_INDEX_NAME2 = "test5_idx";

    protected static final String IDX1_COL = "index 1 col";
    protected static final String IDX2_COL = "index 2 col";
    protected static final String IDX1_VAL = "index 1 val";
    protected static final String IDX2_VAL = "index 2 val";
    protected static final String FIELD1_NAME = "field 1";
    protected static final String FIELD2_NAME = "field 2";
    protected static final String MULTI_VALUE_COLUMN = "multi value col";

    private static CassandraDaemon cassandraService;
    private static Cluster cluster;
    protected static Keyspace dataKeyspace;
    protected static Keyspace indexKeyspace;
    private static Logger logger = Logger.getLogger(AbstractIndexingTest.class);


    @Before
    public void setUp() throws Exception {
        if (cassandraService == null) {
            // Start cassandra server
            cassandraService = new CassandraDaemon();
            cassandraService.activate();
            //Thread.sleep(4000);
            cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, CASSANDRA_HOST + ":" + CASSANDRA_PORT);
            
            // Create indexing schema
            indexKeyspace = createSchema(INDEX_KS, Arrays.asList(CONF_CF, INDEX_CF), cluster);

            // Create data schema
            dataKeyspace = createSchema(DATA_KS, Arrays.asList(DATA_CF, DATA_CF2), cluster);
            cluster.describeKeyspaces();
            
            configureIndexes();
           // Thread.sleep(10000000);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (cassandraService != null) {
            cassandraService.deactivate();
        }
    }

    private Keyspace createSchema(String keyspace, List<String> columnFamilies, Cluster cluster) {
        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace);
        cluster.addKeyspace(newKeyspace, true);
        for (String cf : columnFamilies){
            ColumnFamilyDefinition newCf = HFactory.createColumnFamilyDefinition(keyspace, cf, ComparatorType.UTF8TYPE);
            newCf.setKeyValidationClass( ComparatorType.UTF8TYPE.getTypeName()  );
            newCf.setDefaultValidationClass(ComparatorType.UTF8TYPE.getTypeName());
            cluster.addColumnFamily(newCf);
            logger.debug("Creating column family [" + cf + "] @ [" + keyspace + "]");
        }
    	return HFactory.createKeyspace(keyspace, cluster);

    }

    private void configureIndexes() throws Exception {
        Map<String, String> data = new HashMap<String, String>();
        data.put(Configuration.KEYSPACE, DATA_KS);
        data.put(Configuration.COLUMN_FAMILY, DATA_CF);
        data.put(Configuration.COLUMNS, IDX1_COL + ", " + IDX2_COL);
        persist(indexKeyspace, CONF_CF, INDEX_NAME, data);

        data.clear();
        data.put(Configuration.KEYSPACE, DATA_KS);
        data.put(Configuration.COLUMN_FAMILY, DATA_CF);
        data.put(Configuration.COLUMNS, IDX2_COL);
        persist(indexKeyspace, CONF_CF, INDEX_NAME2, data);

        data.clear();
        data.put(Configuration.KEYSPACE, DATA_KS);
        data.put(Configuration.COLUMN_FAMILY, DATA_CF2);
        data.put(Configuration.COLUMNS, IDX1_COL);
        persist(indexKeyspace, CONF_CF, INDEX_NAME3, data);

        data.clear();
        data.put(Configuration.KEYSPACE, DATA_KS);
        data.put(Configuration.COLUMN_FAMILY, DATA_CF);
        data.put(Configuration.COLUMNS, MULTI_VALUE_COLUMN + ":" + FIELD1_NAME);
        persist(indexKeyspace, CONF_CF, MULTI_VALUE_INDEX_NAME, data);

        data.clear();
        data.put(Configuration.KEYSPACE, DATA_KS);
        data.put(Configuration.COLUMN_FAMILY, DATA_CF);
        data.put(Configuration.COLUMNS, IDX1_COL + ", " + MULTI_VALUE_COLUMN + ":" + FIELD2_NAME);
        persist(indexKeyspace, CONF_CF, MULTI_VALUE_INDEX_NAME2, data);

        DaoFactory.getConfigurationDAO(cluster).getConfiguration().clear();
    }

    protected void persist(Keyspace keyspace, String columnFamily, String rowKey, Map<String, String> columns)
            throws Exception {
    	logger.debug("Writing [" + rowKey + "] in [" + columnFamily + "] @ [" + keyspace.getKeyspaceName() + "]");
        StringSerializer serializer = StringSerializer.get();
        Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);
        for (String columnName : columns.keySet()) {
        	logger.debug("Writing [" + columnName + "] @ [" + rowKey + "] in [" + columnFamily + "] @ [" + keyspace + "]");
            mutator.addInsertion(rowKey, columnFamily, HFactory.createStringColumn(columnName, columns.get(columnName)));
        }
        mutator.execute();
    }

    protected void delete(Keyspace keyspace, String columnFamily, String rowKey, String... columnNames) throws Exception {
        StringSerializer serializer = StringSerializer.get();
        Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);

        for (String columnName : columnNames) {
            mutator.addDeletion(rowKey, columnFamily, columnName, serializer);
        }
        mutator.execute();
    }

    protected void delete(Keyspace keyspace, String columnFamily, String rowKey) throws Exception {
        StringSerializer serializer = StringSerializer.get();
        Mutator<String> mutator = HFactory.createMutator(keyspace, serializer);
        mutator.addDeletion(rowKey, columnFamily);
        mutator.execute();
    }

    protected Map<String, String> select(Keyspace keyspace, String columnFamily, String rowKey) {
        StringSerializer serializer = StringSerializer.get();
        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace,
                columnFamily, serializer, serializer);
        ColumnFamilyResult<String, String> result = template.queryColumns(rowKey);

        Map<String, String> row = new LinkedHashMap<String, String>();
        for (String columnName : result.getColumnNames()) {
        	logger.debug("Fetched [" + columnName + "] in [" + rowKey + "]");
            row.put(columnName, result.getString(columnName));
        }

        return row;
    }

    protected Map<String, Map<String, String>> select(Keyspace keyspace, String columnFamily) {
    	logger.debug("selecting from [" + columnFamily + "] @ [" + keyspace.getKeyspaceName() + "]");
    	
        StringSerializer ss = new StringSerializer();
        RangeSlicesQuery<String, String, String> query = HFactory.createRangeSlicesQuery(keyspace, ss, ss, ss);

        query.setColumnFamily(columnFamily);
        query.setRange("", "", true, 100);
        query.setRowCount(100);
        query.setKeys("", "");
        List<Row<String, String, String>> rows = query.execute().get().getList();
        Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();

        for (Row<String, String, String> row : rows) {
            Map<String, String> cols = new LinkedHashMap<String, String>();
            for (HColumn<String, String> col : row.getColumnSlice().getColumns()) {
                cols.put(col.getName(), col.getValue());
            }
            result.put(row.getKey(), cols);
        }

        return result;
    }
}
