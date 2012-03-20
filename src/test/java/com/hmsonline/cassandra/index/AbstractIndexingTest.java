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
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.junit.Before;

import com.hmsonline.cassandra.index.dao.DAOFactory;
import com.hmsonline.cassandra.index.dao.impl.ConfigurationDAOCassandra;
import com.hmsonline.cassandra.index.dao.impl.IndexDAOCassandra;

public abstract class AbstractIndexingTest {
  protected static final String CLUSTER_NAME = "Test Cluster";
  protected static final String CASSANDRA_HOST = "localhost";
  protected static final int CASSANDRA_PORT = 9160;

  protected static final String INDEX_KS = IndexDAOCassandra.KEYSPACE;
  protected static final String INDEX_CF = IndexDAOCassandra.COLUMN_FAMILY;
  protected static final String CONF_CF = ConfigurationDAOCassandra.COLUMN_FAMILY;
  protected static final String DATA_KS = "ks";
  protected static final String DATA_CF = "cf";
  protected static final String DATA_CF2 = "cf2";
  protected static final String INDEX_NAME = "test_idx";
  protected static final String INDEX_NAME2 = "test2_idx";
  protected static final String INDEX_NAME3 = "test3_idx";

  protected static final String COL1 = "col 1";
  protected static final String COL2 = "col 2";
  protected static final String VAL1 = "val 1";
  protected static final String VAL2 = "val 2";

  protected static final String IDX1_COL = "index 1 col";
  protected static final String IDX2_COL = "index 2 col";
  protected static final String IDX1_VAL = "index 1 val";
  protected static final String IDX2_VAL = "index 2 val";

  protected static final String KEY1 = "key 1";
  protected static final String KEY2 = "key 2";
  protected static final String KEY3 = "key 3";
  protected static final String KEY4 = "key 4";

  private static boolean started = false;
  private static Cluster cluster;

  @Before
  public void setup() throws Exception {
    if (!started) {
      started = true;

      // Start cassandra server
      CassandraDaemon cassandraService = new CassandraDaemon();
      cassandraService.activate();
      cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, CASSANDRA_HOST + ":"
              + CASSANDRA_PORT);

      // Create indexing schema
      CompositeType indexType = CompositeType.getInstance(Arrays.asList(
              (AbstractType) UTF8Type.instance, UTF8Type.instance,
              UTF8Type.instance, UTF8Type.instance, UTF8Type.instance));
      createSchema(INDEX_KS, Arrays.asList(CONF_CF, INDEX_CF),
              Arrays.asList(UTF8Type.instance, (AbstractType) indexType));

      // Create data schema
      createSchema(DATA_KS, Arrays.asList(DATA_CF, DATA_CF2), Arrays.asList(
              (AbstractType) UTF8Type.instance, UTF8Type.instance));

      // Configure test indexes
      configureIndexes();
    }
  }

  private void createSchema(String keyspace, List<String> columnFamilies,
          List<AbstractType> types) {
    CFMetaData[] cfs = new CFMetaData[columnFamilies.size()];
    for (int i = 0; i < columnFamilies.size(); i++) {
      cfs[i] = new CFMetaData(keyspace, columnFamilies.get(i),
              ColumnFamilyType.Standard, types.get(i), null);
    }

    KSMetaData ks = KSMetaData.testMetadata(keyspace, SimpleStrategy.class,
            KSMetaData.optsWithRF(1), cfs);
    Schema.instance.load(Arrays.asList(ks), Schema.instance.getVersion());
  }

  private void configureIndexes() throws Exception {
    Map<String, String> data = new HashMap<String, String>();
    data.put(Configuration.KEYSPACE, DATA_KS);
    data.put(Configuration.COLUMN_FAMILY, DATA_CF);
    data.put(Configuration.COLUMNS, IDX1_COL + ", " + IDX2_COL);
    persist(INDEX_KS, CONF_CF, INDEX_NAME, data);

    data.clear();
    data.put(Configuration.KEYSPACE, DATA_KS);
    data.put(Configuration.COLUMN_FAMILY, DATA_CF);
    data.put(Configuration.COLUMNS, IDX2_COL);
    persist(INDEX_KS, CONF_CF, INDEX_NAME2, data);

    data.clear();
    data.put(Configuration.KEYSPACE, DATA_KS);
    data.put(Configuration.COLUMN_FAMILY, DATA_CF2);
    data.put(Configuration.COLUMNS, IDX1_COL);
    persist(INDEX_KS, CONF_CF, INDEX_NAME3, data);

    DAOFactory.getConfigurationDAO().getConfiguration().clear();
  }

  protected void persist(String keyspace, String columnFamily, String rowKey,
          Map<String, String> columns) throws Exception {
    StringSerializer serializer = StringSerializer.get();
    Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
    Mutator<String> mutator = HFactory
            .createMutator(hectorKeyspace, serializer);

    for (String columnName : columns.keySet()) {
      mutator.addInsertion(rowKey, columnFamily,
              HFactory.createStringColumn(columnName, columns.get(columnName)));
    }
    mutator.execute();
  }

  protected void delete(String keyspace, String columnFamily, String rowKey,
          String... columnNames) throws Exception {
    StringSerializer serializer = StringSerializer.get();
    Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
    Mutator<String> mutator = HFactory
            .createMutator(hectorKeyspace, serializer);

    for (String columnName : columnNames) {
      mutator.addDeletion(rowKey, columnFamily, columnName, serializer);
    }
    mutator.execute();
  }

  protected void delete(String keyspace, String columnFamily, String rowKey)
          throws Exception {
    StringSerializer serializer = StringSerializer.get();
    Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
    Mutator<String> mutator = HFactory
            .createMutator(hectorKeyspace, serializer);
    mutator.addDeletion(rowKey, columnFamily);
    mutator.execute();
  }

  protected Map<String, String> select(String keyspace, String columnFamily,
          String rowKey) {
    StringSerializer serializer = StringSerializer.get();
    Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
    ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(
            hectorKeyspace, columnFamily, serializer, serializer);
    ColumnFamilyResult<String, String> result = template.queryColumns(rowKey);

    Map<String, String> row = new LinkedHashMap<String, String>();
    for (String columnName : result.getColumnNames()) {
      row.put(columnName, result.getString(columnName));
    }

    return row;
  }

  protected Map<String, Map<String, String>> select(String keyspace,
          String columnFamily) {
    StringSerializer ss = new StringSerializer();
    Keyspace hectorKeyspace = HFactory.createKeyspace(keyspace, cluster);
    RangeSlicesQuery<String, String, String> query = HFactory
            .createRangeSlicesQuery(hectorKeyspace, ss, ss, ss);

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
