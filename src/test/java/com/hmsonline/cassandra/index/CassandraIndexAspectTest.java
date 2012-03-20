package com.hmsonline.cassandra.index;

import static com.hmsonline.cassandra.index.util.CompositeUtil.COMPOSITE_SIZE;
import static com.hmsonline.cassandra.index.util.CompositeUtil.decompose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class CassandraIndexAspectTest extends AbstractIndexingTest {
  private Map<String, Map<String, String>> cache = new HashMap<String, Map<String, String>>();

  @Test
  public void testDoIndex() throws Throwable {
    testInsert();
    testDelete();
    testUpdate();
    testMultipleIndexes();
  }

  private void testInsert() throws Throwable {
    // Insert non-index column
    Map<String, String> data = new HashMap<String, String>();
    data.put(COL1, VAL1);
    persist(DATA_KS, DATA_CF, KEY1, data);
    cache.put(KEY1, new HashMap<String, String>(data));

    // Insert index column
    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    persist(DATA_KS, DATA_CF, KEY2, data);
    cache.put(KEY2, new HashMap<String, String>(data));

    // Insert row with index and non-index columns
    data.clear();
    data.put(COL1, VAL1);
    data.put(IDX1_COL, IDX1_VAL);
    persist(DATA_KS, DATA_CF, KEY3, data);
    cache.put(KEY3, new HashMap<String, String>(data));

    // Insert row with multiple index and non-index columns
    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    data.put(COL1, VAL1);
    data.put(IDX2_COL, IDX2_VAL);
    data.put(COL2, VAL2);
    persist(DATA_KS, DATA_CF, KEY4, data);
    cache.put(KEY4, new HashMap<String, String>(data));

    // Assert number of indexes created
    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 3, row.size());

    // Assert index components
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), IDX1_VAL, null, KEY2);
    assertIndex(indexes.next(), IDX1_VAL, null, KEY3);
    assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY4);

    // Assert data in data column family to make sure indexing didn't impact
    // current functionality of Cassandra
    assertData(DATA_KS, DATA_CF, cache);
  }

  private void testDelete() throws Throwable {
    // Delete a row
    delete(DATA_KS, DATA_CF, KEY3);
    cache.get(KEY3).clear();
    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), IDX1_VAL, null, KEY2);
    assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete index column of one-column row
    delete(DATA_KS, DATA_CF, KEY2, IDX1_COL);
    cache.get(KEY2).remove(IDX1_COL);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete non-index column of one-column row
    delete(DATA_KS, DATA_CF, KEY1, COL1);
    cache.get(KEY1).remove(COL1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete index column of multiple-column row
    delete(DATA_KS, DATA_CF, KEY4, IDX1_COL);
    cache.get(KEY4).remove(IDX1_COL);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete non-index column of multiple-column row
    delete(DATA_KS, DATA_CF, KEY4, COL1);
    cache.get(KEY4).remove(COL1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete a row that doesn't exist
    delete(DATA_KS, DATA_CF, KEY1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete index column that doesn't exist
    delete(DATA_KS, DATA_CF, KEY4, IDX1_COL);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete non-index column that doesn't exist
    delete(DATA_KS, DATA_CF, KEY4, COL1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, IDX2_VAL, KEY4);
    assertData(DATA_KS, DATA_CF, cache);

    // Delete all columns of a row
    delete(DATA_KS, DATA_CF, KEY4, COL2, IDX2_COL);
    cache.get(KEY4).clear();
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 0, row.size());
    assertData(DATA_KS, DATA_CF, cache);
  }

  private void testUpdate() throws Throwable {
    // Create test data
    Map<String, String> data = new HashMap<String, String>();
    data.put(COL1, VAL1);
    persist(DATA_KS, DATA_CF, KEY1, data);
    cache.put(KEY1, new HashMap<String, String>(data));

    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    data.put(COL1, VAL1);
    data.put(IDX2_COL, IDX2_VAL);
    data.put(COL2, VAL2);
    persist(DATA_KS, DATA_CF, KEY2, data);
    cache.put(KEY2, new HashMap<String, String>(data));

    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY2);
    assertData(DATA_KS, DATA_CF, cache);

    // Update non-index column
    data.clear();
    data.put(COL1, "new 1");
    persist(DATA_KS, DATA_CF, KEY1, data);
    data.clear();
    data.put(COL2, "new 2");
    persist(DATA_KS, DATA_CF, KEY2, data);
    cache.get(KEY1).put(COL1, "new 1");
    cache.get(KEY2).put(COL2, "new 2");

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY2);
    assertData(DATA_KS, DATA_CF, cache);

    // Update index column
    data.clear();
    data.put(IDX1_COL, "new idx 1");
    persist(DATA_KS, DATA_CF, KEY1, data);
    data.clear();
    data.put(IDX2_COL, "new idx 2");
    persist(DATA_KS, DATA_CF, KEY2, data);
    cache.get(KEY1).put(IDX1_COL, "new idx 1");
    cache.get(KEY2).put(IDX2_COL, "new idx 2");

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), IDX1_VAL, "new idx 2", KEY2);
    assertIndex(indexes.next(), "new idx 1", null, KEY1);
    assertData(DATA_KS, DATA_CF, cache);

    // Update row
    data.clear();
    data.put(IDX1_COL, "update 1");
    data.put(IDX2_COL, "update 2");
    data.put(COL1, "1");
    data.put(COL2, "2");
    data.put("col 3", "3");
    persist(DATA_KS, DATA_CF, KEY2, data);
    cache.put(KEY2, data);

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    indexes = row.keySet().iterator();
    assertIndex(indexes.next(), "new idx 1", null, KEY1);
    assertIndex(indexes.next(), "update 1", "update 2", KEY2);
    assertData(DATA_KS, DATA_CF, cache);

    // Clean up test data
    delete(DATA_KS, DATA_CF, KEY1);
    delete(DATA_KS, DATA_CF, KEY2);
    cache.clear();
  }

  private void testMultipleIndexes() throws Throwable {
    // Test insert
    Map<String, String> data = new HashMap<String, String>();
    data.put(IDX2_COL, IDX2_VAL);
    persist(DATA_KS, DATA_CF, KEY1, data);

    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    persist(DATA_KS, DATA_CF, KEY2, data);

    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    data.put(IDX2_COL, IDX2_VAL);
    persist(DATA_KS, DATA_CF, KEY3, data);

    data.clear();
    data.put(IDX1_COL, IDX1_VAL);
    data.put(IDX2_COL, IDX2_VAL);
    persist(DATA_KS, DATA_CF2, KEY4, data);

    // Assert INDEX 1
    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 3, row.size());
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), null, IDX2_VAL, KEY1);
    assertIndex(indexes.next(), IDX1_VAL, null, KEY2);
    assertIndex(indexes.next(), IDX1_VAL, IDX2_VAL, KEY3);

    // Assert INDEX 2
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME2);
    assertEquals("Number of indexes", 2, row.size());
    indexes = row.keySet().iterator();
    assertIndex(indexes.next(), IDX2_VAL, KEY1);
    assertIndex(indexes.next(), IDX2_VAL, KEY3);

    // Assert INDEX 3
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME3);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, KEY4);

    // Test delete
    delete(DATA_KS, DATA_CF, KEY1, IDX2_COL);
    delete(DATA_KS, DATA_CF, KEY2, IDX1_COL);
    delete(DATA_KS, DATA_CF2, KEY4);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, IDX2_VAL, KEY3);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME2);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX2_VAL, KEY3);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME3);
    assertEquals("Number of indexes", 0, row.size());

    delete(DATA_KS, DATA_CF, KEY3, IDX2_COL);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), IDX1_VAL, null, KEY3);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME2);
    assertEquals("Number of indexes", 0, row.size());

    delete(DATA_KS, DATA_CF, KEY3, IDX1_COL);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 0, row.size());
  }

  private void assertIndex(String index, String... values) throws Throwable {
    List<String> parts = decompose(ByteBufferUtil.bytes(index));
    assertEquals("Number of index components", COMPOSITE_SIZE, parts.size());
    for (int i = 0; i < values.length; i++) {
      assertEquals("Value of component " + i, values[i], parts.get(i));
    }
    for (int i = values.length; i < COMPOSITE_SIZE; i++) {
      assertEquals("Value of component " + i, null, parts.get(i));
    }
  }

  private void assertData(String keyspace, String columnFamily,
          Map<String, Map<String, String>> expectedRows) throws Throwable {
    Map<String, Map<String, String>> actualRows = select(keyspace, columnFamily);
    assertEquals("Number of rows", expectedRows.size(), actualRows.size());

    for (String key : expectedRows.keySet()) {
      Map<String, String> expectedRow = expectedRows.get(key);
      Map<String, String> actualRow = actualRows.get(key);
      assertNotNull("Row [" + key + "]", actualRow);
      assertEquals("Number of columns [" + key + "]", expectedRow.size(),
              actualRow.size());

      for (String col : expectedRow.keySet()) {
        assertEquals("Column value [" + key + ", " + col + "]",
                expectedRow.get(col), actualRow.get(col));
      }
    }
  }
}
