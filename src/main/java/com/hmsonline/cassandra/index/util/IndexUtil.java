package com.hmsonline.cassandra.index.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.Configuration;

public class IndexUtil {
    public static final String INDEXING_KEYSPACE = "Indexing";
    private static Logger logger = LoggerFactory.getLogger(IndexUtil.class);

    public static List<String> buildIndexes(List<String> indexColumns, String rowKey, Map<String, Set<String>> row)
            throws Exception {
        // Calculate number of indexes
        int numIndexes = 0;
        for (String indexColumn : indexColumns) {
            Set<String> values = row.get(indexColumn);
            if (!values.isEmpty()) {
                numIndexes = numIndexes == 0 ? values.size() : numIndexes * values.size();
            }
        }

        if (numIndexes == 0) {
            return new ArrayList<String>();
        }

        // Create empty indexes with the last part filled with rowkey
        List<String[]> indexes = new ArrayList<String[]>();
        for (int i = 0; i < numIndexes; i++) {
            String[] parts = new String[indexColumns.size() + 1];
            parts[parts.length - 1] = rowKey;
            indexes.add(parts);
        }

        // Fill in indexes with component values
        setIndexValues(indexes, indexColumns, row, 0);

        // Build indexes
        List<String> result = new ArrayList<String>();
        for (String[] parts : indexes) {
            result.add(CompositeUtil.compose(Arrays.asList(parts)));
        }

        return result;
    }

    private static void setIndexValues(List<String[]> indexes, List<String> indexColumns, Map<String, Set<String>> row,
            int pos) {
        if (pos >= indexColumns.size()) {
            return;
        }

        Set<String> values = row.get(indexColumns.get(pos));
        if (!values.isEmpty()) {
            int count = 0, n = indexes.size() / values.size();
            for (int i = 0; i < n; i++) {
                for (String value : values) {
                    indexes.get(count++)[pos] = value;
                }
            }
        }

        setIndexValues(indexes, indexColumns, row, pos + 1);
    }

    public static boolean indexChanged(ColumnFamily columnFamily, Collection<String> indexColumns) throws Exception {
        for (ByteBuffer columnName : columnFamily.getColumnNames()) {
            if (contains(indexColumns, ByteBufferUtil.string(columnName))) {
                return true;
            }
        }
        return false;
    }

    private static boolean contains(Collection<String> indexColumns, String columnName) {
        for (String indexColumn : indexColumns) {
            if (isMultiValueColumn(indexColumn)) {
                if (columnName.startsWith(getColumnPrefix(indexColumn))) {
                    return true;
                }
            } else {
                if (columnName.equals(indexColumn)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static Map<String, Set<String>> getIndexValues(Map<String, String> row, Collection<String> indexColumns) {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        for (String indexColumn : indexColumns) {
            Set<String> values = new HashSet<String>();

            if (isMultiValueColumn(indexColumn)) {
                String[] path = indexColumn.split(Configuration.FIELD_DELIM);
                for (String columnName : row.keySet()) {
                    if (columnName.startsWith(path[0]) && StringUtils.isNotEmpty(row.get(columnName))) {
                        values.addAll(getJsonValues(row.get(columnName), path));
                    }
                }
            } else {
                if (StringUtils.isNotEmpty(row.get(indexColumn))) {
                    values.add(row.get(indexColumn));
                }
            }

            result.put(indexColumn, values);
        }

        return result;
    }

    private static List<String> getJsonValues(String jsonString, String[] path) {
        Object json = null;
        try {
            json = new JSONParser().parse(jsonString);
        } catch (Exception ex) {
            logger.warn("Unable to parse json string: " + jsonString, ex);
        }
        return getJsonValues(json, path, 1);
    }

    private static List<String> getJsonValues(Object json, String[] path, int index) {
        if (json == null) {
            return new ArrayList<String>();
        }

        if (index >= path.length) {
            return Arrays.asList(String.valueOf(json));
        }

        if (json instanceof JSONObject) {
            return getJsonValues(((JSONObject) json).get(path[index]), path, index + 1);
        }

        if (json instanceof JSONArray) {
            List<String> values = new ArrayList<String>();
            Iterator<?> iter = ((JSONArray) json).iterator();
            while (iter.hasNext()) {
                values.addAll(getJsonValues(iter.next(), path, index));
            }
            return values;
        }

        logger.warn("Invalid json format: " + json);
        return new ArrayList<String>();
    }

    public static Map<String, String> fetchRow(Cluster cluster, String keyspace,
            String columnFamily, String key, Set<String> indexColumns) throws Exception {
        Keyspace ks = HFactory.createKeyspace(keyspace, cluster);
        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(ks, StringSerializer.get(),
                StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(columnFamily);
        sliceQuery.setKey(key);

        if (!containsMultiValueColumn(indexColumns)) {
            // Fetch specific columns
            List<String> columnNames = new ArrayList<String>();
            for (String indexColumn : indexColumns) {
                columnNames.add(indexColumn);
            }
            String[] varargs = columnNames.toArray(new String[0]);
            sliceQuery.setColumnNames(varargs);
        } else if (indexColumns.size() == 1) {
            // Fetch a slice range
            String columnPrefix = getColumnPrefix(indexColumns.iterator().next());
            sliceQuery.setRange(columnPrefix, columnPrefix + Character.MAX_VALUE, false, 2000);
        } else {
            sliceQuery.setRange("", "", false, 2000); // MAX_COLUMNS
        }
        QueryResult<ColumnSlice<String, String>> row = sliceQuery.execute();
        ColumnSlice<String, String> columns = row.get();

        Map<String, String> result = new HashMap<String, String>();
        for (HColumn<String, String> column : columns.getColumns()) {
            result.put(column.getName(), column.getValue());
        }
        return result;
    }

    public static Map<String, String> getNewRow(Map<String,String> currentRow, ColumnFamily columnFamily)
            throws Exception {
        Map<String, String> mutation = new HashMap<String, String>();
        for (IColumn column : columnFamily.getSortedColumns()) {
            String value = column.isMarkedForDelete() ? null : ByteBufferUtil.string(column.value());
            mutation.put(ByteBufferUtil.string(column.name()), value);
        }

        Map<String, String> newRow = new HashMap<String, String>(currentRow);
        newRow.putAll(mutation);
        return newRow;
    }

    private static boolean containsMultiValueColumn(Collection<String> indexColumns) {
        for (String indexColumn : indexColumns) {
            if (isMultiValueColumn(indexColumn)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isMultiValueColumn(String indexColumn) {
        return indexColumn.indexOf(Configuration.FIELD_DELIM) >= 0;
    }

    private static String getColumnPrefix(String indexColumn) {
        return indexColumn.substring(0, indexColumn.indexOf(Configuration.FIELD_DELIM));
    }
}
