package com.hmsonline.cassandra.index.dao;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.hmsonline.cassandra.index.LogEntry;

public interface CommitLogDao {
    public void writeEntry(LogEntry entry, ConsistencyLevel consistency);
    public void removeEntry(LogEntry entry, ConsistencyLevel consistency);
}
