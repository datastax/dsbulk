/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.Record;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/** */
public class DefaultRecord extends LinkedHashMap<String, Object> implements Record {

  private final Object source;
  private final Supplier<URI> resource;
  private final long position;
  private final Supplier<URI> location;

  public DefaultRecord(
      Object source,
      Supplier<URI> resource,
      long position,
      Supplier<URI> location,
      Object... values) {
    this.source = source;
    this.resource = resource;
    this.position = position;
    this.location = location;
    Streams.forEachPair(
        IntStream.range(0, values.length).boxed().map(Object::toString),
        Arrays.stream(values),
        this::put);
  }

  public DefaultRecord(
      Object source,
      Supplier<URI> resource,
      long position,
      Supplier<URI> location,
      String[] keys,
      Object... values) {
    this.resource = resource;
    this.position = position;
    if (keys.length != values.length) {
      throw new IllegalArgumentException("Keys and values have different sizes");
    }
    this.source = source;
    this.location = location;
    Streams.forEachPair(Arrays.stream(keys), Arrays.stream(values), this::put);
  }

  @Override
  public Object getSource() {
    return source;
  }

  @Override
  public URI getResource() {
    return resource.get();
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public URI getLocation() {
    return location.get();
  }

  @Override
  public Set<String> fields() {
    return keySet();
  }

  @Override
  public Collection<Object> values() {
    return super.values();
  }

  @Override
  public Object getFieldValue(String field) {
    return get(field);
  }

  /**
   * Sets the value associated with the given field.
   *
   * @param field the field name.
   * @param value The value to set.
   */
  public void setFieldValue(String field, Object value) {
    put(field, value);
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
