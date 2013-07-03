package com.hmsonline.cassandra.index;

import static com.hmsonline.cassandra.index.util.CompositeUtil.decompose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.Keyspace;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraIndexAspectTest extends AbstractIndexingTest {
    private static final String KEY1 = "key 1";
    private static final String KEY2 = "key 2";
    private static final String KEY3 = "key 3";
    private static final String KEY4 = "key 4";
    private static final String COL1 = "col 1";
    private static final String COL2 = "col 2";
    private static final String VAL1 = "val 1";
    private static final String VAL2 = "val 2";
    private static final String EMPTY = "";
    
    private static Logger logger = LoggerFactory.getLogger(CassandraIndexAspectTest.class);
    private Map<String, Map<String, String>> cache = new HashMap<String, Map<String, String>>();

    @Test
    public void testIndex() throws Throwable {
        testInsert();

        testDelete();
        testUpdate();
        testMultipleIndexes();
        testMultiValueIndexes();
        testSingleAndMultiValueIndexes();
        logger.debug("TEST COMPLETE!");
//        Thread.sleep(1000000);
    }

    private void testInsert() throws Throwable {
        // Insert non-index column
        Map<String, String> data = new HashMap<String, String>();
        data.put(COL1, VAL1);
        persist(dataKeyspace, DATA_CF, KEY1, data);
        cache.put(KEY1, new HashMap<String, String>(data));

        // Insert index column
        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        persist(dataKeyspace, DATA_CF, KEY2, data);
        cache.put(KEY2, new HashMap<String, String>(data));

        // Insert row with index and non-index columns
        data.clear();
        data.put(COL1, VAL1);
        data.put(IDX1_COL, IDX1_VAL);
        persist(dataKeyspace, DATA_CF, KEY3, data);
        cache.put(KEY3, new HashMap<String, String>(data));

        // Insert row with multiple index and non-index columns
        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        data.put(COL1, VAL1);
        data.put(IDX2_COL, IDX2_VAL);
        data.put(COL2, VAL2);
        persist(dataKeyspace, DATA_CF, KEY4, data);
        cache.put(KEY4, new HashMap<String, String>(data));

        // assert number of indexes created
        Map<String, String> row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        logger.debug("Looking for indexes in ks=[" + indexKeyspace.getKeyspaceName() + "], cf=[" + INDEX_CF +"], key=[" + INDEX_NAME + "]");
        assertEquals("Number of indexes", 3, row.size());
//        Thread.sleep(1000000);

        // assert index components
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY2);
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY3);
        assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY4);

        // assert data in data column family to make sure indexing didn't impact
        // current functionality of Cassandra
        assertData(dataKeyspace, DATA_CF, cache);
    }

    private void testDelete() throws Throwable {
        // Delete a row
        delete(dataKeyspace, DATA_CF, KEY3);
        cache.get(KEY3).clear();
        Map<String, String> row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY2);
        assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete index column from one-column row
        delete(dataKeyspace, DATA_CF, KEY2, IDX1_COL);
        cache.get(KEY2).remove(IDX1_COL);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete non-index column from one-column row
        delete(dataKeyspace, DATA_CF, KEY1, COL1);
        cache.get(KEY1).remove(COL1);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete index column from multiple-column row
        delete(dataKeyspace, DATA_CF, KEY4, IDX1_COL);
        cache.get(KEY4).remove(IDX1_COL);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME); // Switches keyspace to Indexing.
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), EMPTY, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete non-index column from multiple-column row
        delete(dataKeyspace, DATA_CF, KEY4, COL1);
        cache.get(KEY4).remove(COL1);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), EMPTY, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete a row that doesn't exist
        delete(dataKeyspace, DATA_CF, KEY1);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), EMPTY, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete index column that doesn't exist
        delete(dataKeyspace, DATA_CF, KEY4, IDX1_COL);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), EMPTY, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete non-index column that doesn't exist
        delete(dataKeyspace, DATA_CF, KEY4, COL1);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), EMPTY, IDX2_VAL, KEY4);
        assertData(dataKeyspace, DATA_CF, cache);

        // Delete all columns from a row
        delete(dataKeyspace, DATA_CF, KEY4, COL2, IDX2_COL);
        cache.get(KEY4).clear();
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
        assertData(dataKeyspace, DATA_CF, cache);
    }

    private void testUpdate() throws Throwable {
        // Create test data
        Map<String, String> data = new HashMap<String, String>();
        data.put(COL1, VAL1);
        persist(dataKeyspace, DATA_CF, KEY1, data);
        cache.put(KEY1, new HashMap<String, String>(data));

        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        data.put(COL1, VAL1);
        data.put(IDX2_COL, IDX2_VAL);
        data.put(COL2, VAL2);
        persist(dataKeyspace, DATA_CF, KEY2, data);
        cache.put(KEY2, new HashMap<String, String>(data));

        Map<String, String> row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY2);
        assertData(dataKeyspace, DATA_CF, cache);

        // Update non-index column
        data.clear();
        data.put(COL1, "new 1");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        data.clear();
        data.put(COL2, "new 2");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        cache.get(KEY1).put(COL1, "new 1");
        cache.get(KEY2).put(COL2, "new 2");

        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY2);
        assertData(dataKeyspace, DATA_CF, cache);

        // Update index column
        data.clear();
        data.put(IDX1_COL, "new idx 1");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        data.clear();
        data.put(IDX2_COL, "new idx 2");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        cache.get(KEY1).put(IDX1_COL, "new idx 1");
        cache.get(KEY2).put(IDX2_COL, "new idx 2");

        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, "new idx 2", KEY2);
        assertIndex(indexes.next(), "new idx 1", EMPTY, KEY1);
        assertData(dataKeyspace, DATA_CF, cache);

        // Update row
        data.clear();
        data.put(IDX1_COL, "update 1");
        data.put(IDX2_COL, "update 2");
        data.put(COL1, "1");
        data.put(COL2, "2");
        data.put("col 3", "3");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        cache.put(KEY2, data);

        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "new idx 1", EMPTY, KEY1);
        assertIndex(indexes.next(), "update 1", "update 2", KEY2);
        assertData(dataKeyspace, DATA_CF, cache);

        // Clean up test data
        delete(dataKeyspace, DATA_CF, KEY1);
        delete(dataKeyspace, DATA_CF, KEY2);
        cache.clear();
    }

    private void testMultipleIndexes() throws Throwable {
        // Test insert
        Map<String, String> data = new HashMap<String, String>();
        data.put(IDX2_COL, IDX2_VAL);
        persist(dataKeyspace, DATA_CF, KEY1, data);

        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        persist(dataKeyspace, DATA_CF, KEY2, data);

        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        data.put(IDX2_COL, IDX2_VAL);
        persist(dataKeyspace, DATA_CF, KEY3, data);

        data.clear();
        data.put(IDX1_COL, IDX1_VAL);
        data.put(IDX2_COL, IDX2_VAL);
        persist(dataKeyspace, DATA_CF2, KEY4, data);

        // assert INDEX 1
        Map<String, String> row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 3, row.size());
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), EMPTY, IDX2_VAL, KEY1);
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY2);
        assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY3);

        // assert INDEX 2
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME2);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX2_VAL, KEY1);
        assertIndex(indexes.next(), IDX2_VAL, KEY3);

        // assert INDEX 3
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME3);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, KEY4);

        // Test delete
        delete(dataKeyspace, DATA_CF, KEY1, IDX2_COL);
        delete(dataKeyspace, DATA_CF, KEY2, IDX1_COL);
        delete(dataKeyspace, DATA_CF2, KEY4);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY3);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME2);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX2_VAL, KEY3);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME3);
        assertEquals("Number of indexes", 0, row.size());

        delete(dataKeyspace, DATA_CF, KEY3, IDX2_COL);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        assertIndex(row.keySet().iterator().next(), IDX1_VAL, EMPTY, KEY3);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME2);
        assertEquals("Number of indexes", 0, row.size());

        delete(dataKeyspace, DATA_CF, KEY3, IDX1_COL);
        row = select(indexKeyspace, INDEX_CF, INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
    }

    private void testMultiValueIndexes() throws Throwable {
        // Insert json object
        Map<String, String> data = new HashMap<String, String>();
        data.put(MULTI_VALUE_COLUMN + "[1]", "{\"field 1\":\"value 1\", \"field 2\":\"value 2\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        Map<String, String> row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);

        // Insert another value
        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[2]", "{\"field 3\":\"value 3\", \"field 1\":\"value 2\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Insert duplicate value
        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[3]", "{\"field 4\":\"value 4\", \"field 1\":\"value 1\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Insert missing field json
        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[4]", "{\"field 3\":\"value 3\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Insert another row
        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[1]", "{\"field 1\":\"value 1\"}");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 3, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 1", KEY2);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Insert json array
        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[2]",
                "[{\"field 1\":\"value 3\"}, {\"field 2\":\"value 2\", \"field 1\":\"value 4\"}]");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 5, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 1", KEY2);
        assertIndex(indexes.next(), "value 2", KEY1);
        assertIndex(indexes.next(), "value 3", KEY2);
        assertIndex(indexes.next(), "value 4", KEY2);

        // Delete json array column
        delete(dataKeyspace, DATA_CF, KEY2, MULTI_VALUE_COLUMN + "[2]");
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 3, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 1", KEY2);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Delete json object column
        delete(dataKeyspace, DATA_CF, KEY2, MULTI_VALUE_COLUMN + "[1]");
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        assertIndex(indexes.next(), "value 2", KEY1);

        // Delete row
        delete(dataKeyspace, DATA_CF, KEY1);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
    }

    private void testSingleAndMultiValueIndexes() throws Throwable {
        Map<String, String> data = new HashMap<String, String>();
        data.put(IDX1_COL, IDX1_VAL);
        data.put(MULTI_VALUE_COLUMN + "[1]", "{\"field 1\":\"value 1\", \"field 2\":\"value 2\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        Map<String, String> row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        Iterator<String> indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 1", KEY1);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, "value 2", KEY1);

        data.clear();
        data.put(COL1, VAL1);
        data.put(MULTI_VALUE_COLUMN + "[1]", "{\"field 1\":\"value 2\"}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 2", KEY1);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY1);

        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[1]", "{\"field 2\":\"value 2\"}");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 2", KEY1);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), EMPTY, "value 2", KEY2);
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY1);

        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[2]", "{\"field 3\":\"value 3\"}");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), "value 2", KEY1);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), EMPTY, "value 2", KEY2);
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY1);

        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[1]", "{}");
        persist(dataKeyspace, DATA_CF, KEY1, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 2, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), EMPTY, "value 2", KEY2);
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY1);

        data.clear();
        data.put(MULTI_VALUE_COLUMN + "[1]", "{}");
        persist(dataKeyspace, DATA_CF, KEY2, data);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 1, row.size());
        indexes = row.keySet().iterator();
        assertIndex(indexes.next(), IDX1_VAL, EMPTY, KEY1);

        delete(dataKeyspace, DATA_CF, KEY1, IDX1_COL);
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME);
        assertEquals("Number of indexes", 0, row.size());
        row = select(indexKeyspace, INDEX_CF, MULTI_VALUE_INDEX_NAME2);
        assertEquals("Number of indexes", 0, row.size());
    }

    private void assertIndex(String index, String... values) throws Throwable {
        List<String> parts = decompose(ByteBufferUtil.bytes(index));
        assertEquals("Number of index components", values.length, parts.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals("Value of component " + i, values[i], parts.get(i));
        }
    }

    private void assertData(Keyspace keyspace, String columnFamily, Map<String, Map<String, String>> expectedRows)
            throws Throwable {
        Map<String, Map<String, String>> actualRows = select(keyspace, columnFamily);
        assertEquals("Number of rows", expectedRows.size(), actualRows.size());

        for (String key : expectedRows.keySet()) {
            Map<String, String> expectedRow = expectedRows.get(key);
            Map<String, String> actualRow = actualRows.get(key);
            assertNotNull("Row [" + key + "]", actualRow);
            assertEquals("Number of columns [" + key + "]", expectedRow.size(), actualRow.size());

            for (String col : expectedRow.keySet()) {
                assertEquals("Column value [" + key + ", " + col + "]", expectedRow.get(col), actualRow.get(col));
            }
        }
    }
}
