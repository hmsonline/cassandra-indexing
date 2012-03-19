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
  }

  private void testInsert() throws Throwable {
    // Insert non-index column
    Map<String, String> data = new HashMap<String, String>();
    data.put("col 1", "val 1");
    persist(DATA_KS, DATA_CF1, "KEY1", data);
    cache.put("KEY1", new HashMap<String, String>(data));

    // Insert index column
    data.clear();
    data.put(INDEX_1, "index 1 val");
    persist(DATA_KS, DATA_CF1, "KEY2", data);
    cache.put("KEY2", new HashMap<String, String>(data));

    // Insert row with index and non-index columns
    data.clear();
    data.put("col 1", "val 1");
    data.put(INDEX_1, "index 1 val");
    persist(DATA_KS, DATA_CF1, "KEY3", data);
    cache.put("KEY3", new HashMap<String, String>(data));

    // Insert row with multiple index and non-index columns
    data.clear();
    data.put(INDEX_1, "index 1 val");
    data.put("col 1", "val 1");
    data.put(INDEX_2, "index 2 val");
    data.put("col 2", "val 2");
    persist(DATA_KS, DATA_CF1, "KEY4", data);
    cache.put("KEY4", new HashMap<String, String>(data));

    // Assert number of indexes created
    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 3, row.size());

    // Assert index components
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), "index 1 val", null, "KEY2");
    assertIndex(indexes.next(), "index 1 val", null, "KEY3");
    assertIndex(indexes.next(), "index 1 val", "index 2 val", "KEY4");

    // Assert data in data column family to make sure indexing didn't impact
    // current functionality of Cassandra
    assertData(DATA_KS, DATA_CF1, cache);
  }

  private void testDelete() throws Throwable {
    // Delete a row
    delete(DATA_KS, DATA_CF1, "KEY3");
    cache.get("KEY3").clear();
    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), "index 1 val", null, "KEY2");
    assertIndex(indexes.next(), "index 1 val", "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete index column of one-column row
    delete(DATA_KS, DATA_CF1, "KEY2", INDEX_1);
    cache.get("KEY2").remove(INDEX_1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), "index 1 val", "index 2 val",
            "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete non-index column of one-column row
    delete(DATA_KS, DATA_CF1, "KEY1", "col 1");
    cache.get("KEY1").remove("col 1");
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), "index 1 val", "index 2 val",
            "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete index column of multiple-column row
    delete(DATA_KS, DATA_CF1, "KEY4", INDEX_1);
    cache.get("KEY4").remove(INDEX_1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete non-index column of multiple-column row
    delete(DATA_KS, DATA_CF1, "KEY4", "col 1");
    cache.get("KEY4").remove("col 1");
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete a row that doesn't exist
    delete(DATA_KS, DATA_CF1, "KEY1");
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete index column that doesn't exist
    delete(DATA_KS, DATA_CF1, "KEY4", INDEX_1);
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete non-index column that doesn't exist
    delete(DATA_KS, DATA_CF1, "KEY4", "col 1");
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), null, "index 2 val", "KEY4");
    assertData(DATA_KS, DATA_CF1, cache);

    // Delete all columns of a row
    delete(DATA_KS, DATA_CF1, "KEY4", "col 2", INDEX_2);
    cache.get("KEY4").clear();
    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 0, row.size());
    assertData(DATA_KS, DATA_CF1, cache);
  }

  private void testUpdate() throws Throwable {
    // Create test data
    Map<String, String> data = new HashMap<String, String>();
    data.put("col 1", "val 1");
    persist(DATA_KS, DATA_CF1, "KEY1", data);
    cache.put("KEY1", new HashMap<String, String>(data));

    data.clear();
    data.put(INDEX_1, "index 1 val");
    data.put("col 1", "val 1");
    data.put(INDEX_2, "index 2 val");
    data.put("col 2", "val 2");
    persist(DATA_KS, DATA_CF1, "KEY2", data);
    cache.put("KEY2", new HashMap<String, String>(data));

    Map<String, String> row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), "index 1 val", "index 2 val",
            "KEY2");
    assertData(DATA_KS, DATA_CF1, cache);

    // Update non-index column
    data.clear();
    data.put("col 1", "new 1");
    persist(DATA_KS, DATA_CF1, "KEY1", data);
    data.clear();
    data.put("col 2", "new 2");
    persist(DATA_KS, DATA_CF1, "KEY2", data);
    cache.get("KEY1").put("col 1", "new 1");
    cache.get("KEY2").put("col 2", "new 2");

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 1, row.size());
    assertIndex(row.keySet().iterator().next(), "index 1 val", "index 2 val",
            "KEY2");
    assertData(DATA_KS, DATA_CF1, cache);

    // Update index column
    data.clear();
    data.put(INDEX_1, "idx val 1");
    persist(DATA_KS, DATA_CF1, "KEY1", data);
    data.clear();
    data.put(INDEX_2, "idx val 2");
    persist(DATA_KS, DATA_CF1, "KEY2", data);
    cache.get("KEY1").put(INDEX_1, "idx val 1");
    cache.get("KEY2").put(INDEX_2, "idx val 2");

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    Iterator<String> indexes = row.keySet().iterator();
    assertIndex(indexes.next(), "idx val 1", null, "KEY1");
    assertIndex(indexes.next(), "index 1 val", "idx val 2", "KEY2");
    assertData(DATA_KS, DATA_CF1, cache);

    // Update row
    data.clear();
    data.put(INDEX_1, "update 1");
    data.put(INDEX_2, "update 2");
    data.put("col 1", "1");
    data.put("col 2", "2");
    data.put("col 3", "3");
    persist(DATA_KS, DATA_CF1, "KEY2", data);
    cache.put("KEY2", data);

    row = select(INDEX_KS, INDEX_CF, INDEX_NAME);
    assertEquals("Number of indexes", 2, row.size());
    indexes = row.keySet().iterator();
    assertIndex(indexes.next(), "idx val 1", null, "KEY1");
    assertIndex(indexes.next(), "update 1", "update 2", "KEY2");
    assertData(DATA_KS, DATA_CF1, cache);
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
