package com.hmsonline.cassandra.index.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompositeUtil {
  public static final char COMPOSITE_DELIM = Character.MIN_VALUE;

  public static ByteBuffer compose(List<String> parts)
          throws ConfigurationException {
    StringBuffer buf = new StringBuffer();
    for (String part : parts) {
      buf.append(COMPOSITE_DELIM).append(part == null ? "" : part);
    }
    if (buf.length() > 0) {
      buf.deleteCharAt(0);
    }
    return ByteBufferUtil.bytes(buf.toString());
  }

  public static List<String> decompose(ByteBuffer value)
          throws ConfigurationException, CharacterCodingException {
    String[] parts = ByteBufferUtil.string(value).split(
            String.valueOf(COMPOSITE_DELIM));
    return Arrays.asList(parts);
  }
}