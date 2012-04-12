package com.hmsonline.cassandra.index;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONSerializer;

import org.apache.cassandra.utils.UUIDGen;
import org.json.simple.JSONObject;

@SuppressWarnings("unchecked")
public class LogEntry {
    public enum Status {
        PENDING, ERROR;
    }

    private String entryKey;
    private String entryName;
    private JSONObject entryValue;
    private static String hostName;

    public LogEntry(String json) {
        this.entryKey = generateEntryKey();
        this.entryName = generateEntryName();
        this.entryValue = (JSONObject) JSONSerializer.toJSON(json);
    }

    public LogEntry(String keyspace, String columnFamily, String rowKey, Status status, Map<String, String> mutation) {
        this.entryKey = generateEntryKey();
        this.entryName = generateEntryName();
        this.entryValue = new JSONObject();

        this.setHost(getHostName());
        this.setKeyspace(keyspace);
        this.setColumnFamily(columnFamily);
        this.setRowKey(rowKey);
        this.setStatus(status);
        this.setMutation(mutation);
        this.setTimestamp(String.valueOf(System.currentTimeMillis()));
    }

    public void setHost(String host) {
        entryValue.put("host", host);
    }

    public String getHost() {
        return (String) entryValue.get("host");
    }

    public void setKeyspace(String keyspace) {
        entryValue.put("keyspace", keyspace);
    }

    public String getKeyspace() {
        return (String) entryValue.get("keyspace");
    }

    public void setColumnFamily(String columnFamily) {
        entryValue.put("column_family", columnFamily);
    }

    public String getColumnFamily() {
        return (String) entryValue.get("column_family");
    }

    public void setRowKey(String rowKey) {
        entryValue.put("rowkey", rowKey);
    }

    public String getRowKey() {
        return (String) entryValue.get("rowkey");
    }

    public void setStatus(Status status) {
        entryValue.put("status", status.toString());
    }

    public Status getStatus() {
        return Status.valueOf((String) entryValue.get("status"));
    }

    public void setTimestamp(String timestamp) {
        entryValue.put("timestamp", timestamp);
    }

    public String getTimestamp() {
        return (String) entryValue.get("timestamp");
    }

    public void setMutation(Map<String, String> mutation) {
        JSONObject json = new JSONObject();
        for (String key : mutation.keySet()) {
            json.put(key, mutation.get(key));
        }
        entryValue.put("mutation", mutation);
    }

    public Map<String, String> getMutation() {
        JSONObject json = (JSONObject) entryValue.get("mutation");
        Map<String, String> mutation = new HashMap<String, String>();
        for (Object key : json.keySet()) {
            mutation.put((String) key, (String) json.get(key));
        }
        return mutation;
    }

    public String getMessage() {
        return (String) entryValue.get("message");
    }

    public void setMessage(String reason) {
        entryValue.put("message", reason);
    }

    public String getEntryKey() {
        return entryKey;
    }

    public String getEntryName() {
        return entryName;
    }

    public String getEntryValue() {
        return entryValue.toJSONString();
    }

    private String getHostName() {
        if (hostName == null) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (hostName == null && interfaces.hasMoreElements()) {
                    Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
                    while (hostName == null && addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress()) {
                            hostName = address.getHostName();
                        }
                    }
                }
            } catch (SocketException ex) {
                throw new RuntimeException(ex);
            }
        }

        return hostName;
    }

    private String generateEntryKey() {
        return String.valueOf(System.currentTimeMillis() / (60 * 60 * 1000));
    }

    private String generateEntryName() {
        return UUIDGen.getUUID(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())).toString();
    }
}
