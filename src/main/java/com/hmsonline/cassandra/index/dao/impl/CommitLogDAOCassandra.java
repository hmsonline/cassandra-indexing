package com.hmsonline.cassandra.index.dao.impl;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.hmsonline.cassandra.index.LogEntry;
import com.hmsonline.cassandra.index.dao.CommitLogDao;
import com.hmsonline.cassandra.index.util.IndexUtil;

public class CommitLogDaoCassandra extends AbstractCassandraDao implements CommitLogDao {
    public static final String KEYSPACE = IndexUtil.INDEXING_KEYSPACE;
    public static final String COLUMN_FAMILY = "CommitLog";

    public CommitLogDaoCassandra() {
        super(KEYSPACE, COLUMN_FAMILY);
    }

    public void writeEntry(LogEntry entry, ConsistencyLevel consistency) {
        try {
            insertColumn(ByteBufferUtil.bytes(entry.getEntryKey()), ByteBufferUtil.bytes(entry.getEntryName()),
                    ByteBufferUtil.bytes(entry.getEntryValue()), consistency);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to write commit log entry: " + entry.getEntryKey() + "["
                    + entry.getEntryName() + ", " + entry.getEntryValue() + "]", ex);
        }
    }

    public void removeEntry(LogEntry entry, ConsistencyLevel consistency) {
        try {
            deleteColumn(ByteBufferUtil.bytes(entry.getEntryKey()), ByteBufferUtil.bytes(entry.getEntryName()),
                    consistency);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to remove commit log entry: " + entry.getEntryKey() + "["
                    + entry.getEntryName() + "]", ex);
        }
    }
}
