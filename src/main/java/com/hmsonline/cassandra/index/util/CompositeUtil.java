//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.cassandra.index.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.config.ConfigurationException;

public class CompositeUtil {
  public static final int COMPOSITE_SIZE = 5;

  public static ByteBuffer compose(List<String> parts)
          throws ConfigurationException {
    return compose(parts, true);
  }

  public static ByteBuffer compose(List<String> parts, boolean appendMissing)
          throws ConfigurationException {
    Composite composite = new Composite();
    StringSerializer ss = StringSerializer.get();
    for (String part : parts) {
      composite.addComponent(part, ss, "UTF8Type");
    }

    if (appendMissing) {
      for (int i = 0; i < COMPOSITE_SIZE - parts.size(); i++) {
        composite.addComponent(null, ss, "UTF8Type");
      }
    }

    return composite.serialize();
  }

  public static List<String> decompose(ByteBuffer value)
          throws ConfigurationException, CharacterCodingException {
    List<String> parts = new ArrayList<String>();
    if (value == null) {
      return parts;
    }

    Composite composite = new Composite();
    composite.deserialize(value);
    StringSerializer ss = StringSerializer.get();

    for (int i = 0; i < composite.getComponents().size(); i++) {
      Component<ByteBuffer> component = composite.getComponent(i);
      parts.add(component.getValue(ss));
    }

    return parts;
  }
}