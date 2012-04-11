package com.hmsonline.cassandra.index;

import static com.hmsonline.cassandra.index.util.IndexUtil.INDEXING_KEYSPACE;
import static com.hmsonline.cassandra.index.util.IndexUtil.buildIndex;
import static com.hmsonline.cassandra.index.util.IndexUtil.fetchRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.getLogEntries;
import static com.hmsonline.cassandra.index.util.IndexUtil.getNewRow;
import static com.hmsonline.cassandra.index.util.IndexUtil.indexChanged;
import static com.hmsonline.cassandra.index.util.IndexUtil.isEmptyIndex;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.index.LogEntry.Status;
import com.hmsonline.cassandra.index.dao.CommitLogDao;
import com.hmsonline.cassandra.index.dao.ConfigurationDao;
import com.hmsonline.cassandra.index.dao.DaoFactory;
import com.hmsonline.cassandra.index.dao.IndexDao;

/**
 * Aspect class used to handle wide row indexing for Cassandra.
 * 
 * @author pnguyen
 */
@Aspect
public class CassandraIndexAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraIndexAspect.class);
    private IndexDao indexDao = DaoFactory.getIndexDAO();
    private ConfigurationDao configurationDao = DaoFactory.getConfigurationDAO();
    private CommitLogDao commitLogDao = DaoFactory.getCommitLogDAO();

    @Around("execution(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void process(ProceedingJoinPoint joinPoint) throws Throwable {
        ConsistencyLevel consistency = (ConsistencyLevel) joinPoint.getArgs()[0];
        @SuppressWarnings("unchecked")
        List<IMutation> mutations = (List<IMutation>) joinPoint.getArgs()[1];
        List<LogEntry> logEntries = getLogEntries(mutations);
        Configuration conf = configurationDao.getConfiguration();

        try {
            if (conf.isCommitLogEnabled()) {
                writeCommitLog(logEntries, consistency);
            }
            index(mutations, consistency);
            joinPoint.proceed(joinPoint.getArgs());
            if (conf.isCommitLogEnabled()) {
                removeCommitLog(logEntries, consistency);
            }
        } catch (Throwable t) {
            logger.error("An error occurred while handling indexing.", t);
            writeCommitLog(logEntries, consistency, t);
        }
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
                Map<String, Set<String>> configuredIndexes = conf.getIndexes(keyspace, cfName);
                if (configuredIndexes.isEmpty()) {
                    continue;
                }

                // Get current and new values for all index columns
                Set<String> allIndexColumns = new HashSet<String>();
                for (Set<String> columns : configuredIndexes.values()) {
                    allIndexColumns.addAll(columns);
                }
                Map<String, String> currentRow = fetchRow(keyspace, cfName, rowKey, allIndexColumns);
                Map<String, String> newRow = getNewRow(currentRow, cf, allIndexColumns);

                // Iterate over configured indexes and create index for each
                for (String indexName : configuredIndexes.keySet()) {
                    Set<String> indexColumns = configuredIndexes.get(indexName);

                    if (cf.isMarkedForDelete()) {
                        indexDao.deleteIndex(indexName, buildIndex(indexColumns, rowKey, currentRow), consistency);
                    } else if (indexChanged(indexColumns, cf)) {
                        if (!isEmptyIndex(indexColumns, currentRow)) {
                            indexDao.deleteIndex(indexName, buildIndex(indexColumns, rowKey, currentRow), consistency);
                        }
                        if (!isEmptyIndex(indexColumns, newRow)) {
                            indexDao.insertIndex(indexName, buildIndex(indexColumns, rowKey, newRow), consistency);
                        }
                    }
                }
            }
        }
    }

    private void writeCommitLog(List<LogEntry> entries, ConsistencyLevel consistency) {
        for (LogEntry entry : entries) {
            commitLogDao.writeEntry(entry, consistency);
        }
    }

    private void writeCommitLog(List<LogEntry> entries, ConsistencyLevel consistency, Throwable t) {
        Writer writer = new StringWriter();
        t.printStackTrace(new PrintWriter(writer));
        String msg = writer.toString();

        for (LogEntry entry : entries) {
            entry.setStatus(Status.ERROR);
            entry.setMessage(msg);
        }
        writeCommitLog(entries, consistency);
    }

    private void removeCommitLog(List<LogEntry> logEntries, ConsistencyLevel consistency) {
        for (LogEntry entry : logEntries) {
            commitLogDao.removeEntry(entry, consistency);
        }
    }
}
