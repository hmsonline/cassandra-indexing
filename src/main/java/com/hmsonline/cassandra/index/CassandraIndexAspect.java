package com.hmsonline.cassandra.index;

import static com.hmsonline.cassandra.index.util.IndexUtil.INDEXING_KEYSPACE;
import static com.hmsonline.cassandra.index.util.IndexUtil.buildIndex;
import static com.hmsonline.cassandra.index.util.IndexUtil.getNewRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.getRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.indexChanged;
import static com.hmsonline.cassandra.index.util.IndexUtil.isEmptyRow;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.dao.ConfigurationDAO;
import com.hmsonline.cassandra.index.dao.DAOFactory;
import com.hmsonline.cassandra.index.dao.IndexDAO;

/**
 * Aspect class used to handle wide row indexing for Cassandra.
 * 
 * @author pnguyen
 */
@Aspect
public class CassandraIndexAspect {
  private static Logger logger = LoggerFactory
          .getLogger(CassandraIndexAspect.class);
  private IndexDAO indexDao = DAOFactory.getIndexDA0();
  private ConfigurationDAO configurationDao = DAOFactory.getConfigurationDAO();

  @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
  public void doIndex(ProceedingJoinPoint joinPoint) throws Throwable {
    ConsistencyLevel consistency = (ConsistencyLevel) joinPoint.getArgs()[0];
    List<IMutation> mutations = (List<IMutation>) joinPoint.getArgs()[1];
    Configuration config = configurationDao.getConfiguration();

    for (IMutation mutation : mutations) {
      String keyspace = mutation.getTable();
      String rowKey = ByteBufferUtil.string(mutation.key());
      Collection<ColumnFamily> cfs = ((RowMutation) mutation)
              .getColumnFamilies();

      if (INDEXING_KEYSPACE.equals(keyspace)) {
        continue;
      }

      // Iterate over mutated column families and create indexes for each
      for (ColumnFamily cf : cfs) {
        String columnFamily = cf.metadata().cfName;
        Map<String, Set<String>> configuredIndexes = config.getIndexes(
                keyspace, columnFamily);
        if (configuredIndexes.isEmpty()) {
          continue;
        }

        Map<String, String> currentRow = getRow(keyspace, columnFamily, rowKey,
                consistency);
        Map<String, String> newRow = getNewRow(currentRow, cf);

        for (String indexName : configuredIndexes.keySet()) {
          Set<String> indexColumns = configuredIndexes.get(indexName);

          if (cf.isMarkedForDelete()) { // Delete row
            logger.debug("Deleting index: " + indexName + "[" + rowKey + "]");
            indexDao.deleteIndex(indexName,
                    buildIndex(indexColumns, rowKey, currentRow), consistency);
          }
          else if (currentRow.isEmpty()) { // Insert row
            if (!isEmptyRow(newRow)) {
              logger.debug("Inserting index: " + indexName + "[" + rowKey + "]");
              indexDao.insertIndex(indexName,
                      buildIndex(indexColumns, rowKey, newRow), consistency);
            }
          }
          else { // Update row
            logger.debug("Updating index: " + indexName + "[" + rowKey + "]");

            if (isEmptyRow(newRow)) {
              // Empty row will end up with deletion so all its indexes must be
              // deleted accordingly
              indexDao.deleteIndex(indexName,
                      buildIndex(indexColumns, rowKey, currentRow), consistency);
            }
            else if (indexChanged(indexColumns, cf)) {
              // Only update index if at least one index column value changes
              indexDao.deleteIndex(indexName,
                      buildIndex(indexColumns, rowKey, currentRow), consistency);
              indexDao.insertIndex(indexName,
                      buildIndex(indexColumns, rowKey, newRow), consistency);
            }
          }
        }
      }
    }

    // Proceed with Cassandra insert operation
    joinPoint.proceed(joinPoint.getArgs());
  }
}
