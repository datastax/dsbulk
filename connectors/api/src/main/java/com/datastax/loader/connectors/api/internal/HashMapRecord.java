/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.MappedRecord;
import com.google.common.base.MoreObjects;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** */
public class HashMapRecord extends HashMap<String, Object> implements MappedRecord {

  private final Object source;
  private final URL location;

  public HashMapRecord(Object source, URL location, Map<String, Object> values) {
    super(values);
    this.source = source;
    this.location = location;
  }

  @Override
  public Object getSource() {
    return source;
  }

  @Override
  public URL getLocation() {
    return location;
  }

  @Override
  public Set<String> fieldNames() {
    return keySet();
  }

  @Override
  public Object getValue(String fieldName) {
    return get(fieldName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("location", location)
        .toString();
  }
}
