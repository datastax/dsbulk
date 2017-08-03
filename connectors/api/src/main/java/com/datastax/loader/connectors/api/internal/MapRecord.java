/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api.internal;

import com.datastax.loader.connectors.api.MappedRecord;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/** */
public class MapRecord extends LinkedHashMap<Object, Object> implements MappedRecord {

  private final Object source;
  private final URL location;

  public MapRecord(Object source, URL location, Map<Object, Object> values) {
    super(values);
    this.source = source;
    this.location = location;
    Streams.forEachPair(IntStream.range(0, size()).boxed(), values().stream(), this::put);
  }

  public MapRecord(Object source, URL location, Object... values) {
    super();
    this.source = source;
    this.location = location;
    Streams.forEachPair(
        IntStream.range(0, values.length).boxed(), Arrays.stream(values), this::put);
  }

  public MapRecord(Object source, URL location, Object[] keys, Object[] values) {
    this(source, location, values);
    if (keys.length != values.length)
      throw new IllegalArgumentException("Keys and values have different sizes");
    Streams.forEachPair(Arrays.stream(keys), Arrays.stream(values), this::put);
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
  public Set<Object> fields() {
    return keySet();
  }

  @Override
  public Object getFieldValue(Object field) {
    return get(field);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("location", location)
        .add("entries", entrySet())
        .toString();
  }
}
