package com.hmsonline.cassandra.index;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.mortbay.log.Log;

import com.hmsonline.cassandra.index.dao.ConfigurationDao;
import com.hmsonline.cassandra.index.dao.DaoFactory;
import com.hmsonline.cassandra.index.dao.IndexDao;
import com.hmsonline.cassandra.index.util.IndexUtil;

@Aspect
public class CassandraIndexAspect {
    protected static final String CLUSTER_NAME = "Indexing";
    private IndexDao indexDao;
    private ConfigurationDao configurationDao;
    private ExecutorService executors = Executors.newCachedThreadPool();
    private Cluster cluster;

    public CassandraIndexAspect(){
        String cassandraHost = System.getProperty("cassandra.host");
        if (cassandraHost == null){
            Log.debug("No cassandra host specified in environment (-Dcassandra.host), defaulting to localhost:9160");
            cassandraHost = "localhost:9160";
        }
        cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, cassandraHost);
        indexDao = DaoFactory.getIndexDAO(cluster);
        configurationDao = DaoFactory.getConfigurationDAO(cluster);
    }
    
    @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void process(ProceedingJoinPoint joinPoint) throws Throwable {
        ConsistencyLevel consistency = (ConsistencyLevel) joinPoint.getArgs()[0];
        @SuppressWarnings("unchecked")
        List<IMutation> mutations = (List<IMutation>) joinPoint.getArgs()[1];
        Handler handler = new Handler(cluster, indexDao, configurationDao, mutations, consistency);
        Future<?> future = executors.submit(handler);
        future.get();
        joinPoint.proceed(joinPoint.getArgs());
    }

    class Handler implements Runnable {
        private final IndexDao indexDao;
        private final ConfigurationDao configurationDao;
        private final List<IMutation> mutations;
        private final ConsistencyLevel consistency;

        Handler(Cluster cluster, IndexDao indexDao, ConfigurationDao configurationDao, List<IMutation> mutations,
                ConsistencyLevel consistency) {
            this.indexDao = indexDao;
            this.configurationDao = configurationDao;
            this.mutations = mutations;
            this.consistency = consistency;
        }

        public void run() {
            Configuration conf = configurationDao.getConfiguration();
            try {
                for (IMutation mutation : mutations) {
                    String keyspace = mutation.getTable();
                    String rowKey = ByteBufferUtil.string(mutation.key());
                    Collection<ColumnFamily> cfs = ((RowMutation) mutation).getColumnFamilies();
                    if (IndexUtil.INDEXING_KEYSPACE.equals(keyspace)) {
                        continue;
                    }

                    // Iterate over mutated column families and create indexes
                    // for
                    // each
                    for (ColumnFamily cf : cfs) {
                        String cfName = cf.metadata().cfName;
                        Map<String, List<String>> configuredIndexes = conf.getIndexes(keyspace, cfName);
                        if (configuredIndexes.isEmpty()) {
                            continue;
                        }

                        // Get all index columns configured for this column
                        // family
                        Set<String> cfIndexColumns = new HashSet<String>();
                        for (List<String> columns : configuredIndexes.values()) {
                            cfIndexColumns.addAll(columns);
                        }

                        // Skip indexing if none of index columns changed
                        if (!cf.isMarkedForDelete() && !IndexUtil.indexChanged(cf, cfIndexColumns)) {
                            continue;
                        }

                        Map<String, String> currentRow = IndexUtil.fetchRow(cluster, keyspace, cfName, rowKey, cfIndexColumns);
                        Map<String, String> newRow = IndexUtil.getNewRow(currentRow, cf);
                        Map<String, List<String>> currentIndexValues = IndexUtil.getIndexValues(currentRow, cfIndexColumns);
                        Map<String, List<String>> newIndexValues = IndexUtil.getIndexValues(newRow, cfIndexColumns);

                        for (String indexName : configuredIndexes.keySet()) {
                            List<String> indexColumns = configuredIndexes.get(indexName);
                            long timestamp = System.currentTimeMillis() * 1000;
                            if (cf.isMarkedForDelete()) {
                                indexDao.deleteIndexes(indexName,
                                        IndexUtil.buildIndexes(indexColumns, rowKey, currentIndexValues), consistency, timestamp);
                            } else if (IndexUtil.indexChanged(cf, indexColumns)) {
                                indexDao.deleteIndexes(indexName,
                                        IndexUtil.buildIndexes(indexColumns, rowKey, currentIndexValues), consistency, timestamp);
                                indexDao.insertIndexes(indexName, IndexUtil.buildIndexes(indexColumns, rowKey, newIndexValues),
                                        consistency, (timestamp + 1));
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException("Could not index a mutation.", t);
            }
        }
    }
}
