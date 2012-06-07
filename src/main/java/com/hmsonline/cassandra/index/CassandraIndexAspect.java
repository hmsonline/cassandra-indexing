package com.hmsonline.cassandra.index;

import static com.hmsonline.cassandra.index.util.IndexUtil.INDEXING_KEYSPACE;
import static com.hmsonline.cassandra.index.util.IndexUtil.buildIndexes;
import static com.hmsonline.cassandra.index.util.IndexUtil.fetchRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.getIndexValues;
import static com.hmsonline.cassandra.index.util.IndexUtil.getNewRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.indexChanged;

import java.util.Collection;
import java.util.HashSet;
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

import com.hmsonline.cassandra.index.dao.ConfigurationDao;
import com.hmsonline.cassandra.index.dao.DaoFactory;
import com.hmsonline.cassandra.index.dao.IndexDao;

@Aspect
public class CassandraIndexAspect {
    private IndexDao indexDao = DaoFactory.getIndexDAO();
    private ConfigurationDao configurationDao = DaoFactory.getConfigurationDAO();

    @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void process(ProceedingJoinPoint joinPoint) throws Throwable {
        ConsistencyLevel consistency = (ConsistencyLevel) joinPoint.getArgs()[0];
        @SuppressWarnings("unchecked")
        List<IMutation> mutations = (List<IMutation>) joinPoint.getArgs()[1];
        index(mutations, consistency);
        joinPoint.proceed(joinPoint.getArgs());
    }

    private void index(List<IMutation> mutations, ConsistencyLevel consistency) throws Throwable {
        Configuration conf = configurationDao.getConfiguration();

        for (IMutation mutation : mutations) {
            String keyspace = mutation.getTable();
            String rowKey = ByteBufferUtil.string(mutation.key());
            Collection<ColumnFamily> cfs = ((RowMutation) mutation).getColumnFamilies();
            if (INDEXING_KEYSPACE.equals(keyspace)) {
                continue;
            }

            // Iterate over mutated column families and create indexes for each
            for (ColumnFamily cf : cfs) {
                String cfName = cf.metadata().cfName;
                Map<String, List<String>> configuredIndexes = conf.getIndexes(keyspace, cfName);
                if (configuredIndexes.isEmpty()) {
                    continue;
                }

                // Get all index columns configured for this column family
                Set<String> cfIndexColumns = new HashSet<String>();
                for (List<String> columns : configuredIndexes.values()) {
                    cfIndexColumns.addAll(columns);
                }

                // Skip indexing if none of index columns changed
                if (!cf.isMarkedForDelete() && !indexChanged(cf, cfIndexColumns)) {
                    continue;
                }

                Map<String, String> currentRow = fetchRow(keyspace, cfName, rowKey, cfIndexColumns);
                Map<String, String> newRow = getNewRow(currentRow, cf);
                Map<String, Set<String>> currentIndexValues = getIndexValues(currentRow, cfIndexColumns);
                Map<String, Set<String>> newIndexValues = getIndexValues(newRow, cfIndexColumns);

                // Iterate over configured indexes and create indexes for each
                for (String indexName : configuredIndexes.keySet()) {
                    List<String> indexColumns = configuredIndexes.get(indexName);
                    long timestamp = System.currentTimeMillis() * 1000;
                    if (cf.isMarkedForDelete()) {
                        indexDao.deleteIndexes(indexName, buildIndexes(indexColumns, rowKey, currentIndexValues),
                                consistency, timestamp);
                    } else if (indexChanged(cf, indexColumns)) {
                        indexDao.deleteIndexes(indexName, buildIndexes(indexColumns, rowKey, currentIndexValues),
                                consistency, timestamp);
                        indexDao.insertIndexes(indexName, buildIndexes(indexColumns, rowKey, newIndexValues),
                                consistency, (timestamp + 1)); //Add one for insert in case of a tie.
                    }
                }
            }
        }
    }
}
