/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.connectors.api;

import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.driver.shaded.guava.common.base.Suppliers;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class DefaultRecord extends LinkedHashMap<Field, Object> implements Record {

  /**
   * Creates an indexed record with the given values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param values the record values.
   * @return an indexed record.
   */
  public static DefaultRecord indexed(
      @NonNull Object source, @NonNull URI resource, long position, Object... values) {
    return new DefaultRecord(source, resource, position, values);
  }

  /**
   * Creates a mapped record with the given keys and values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param keys the record keys.
   * @param values the record values.
   * @return a mapped record.
   */
  public static DefaultRecord mapped(
      @NonNull Object source,
      @NonNull URI resource,
      long position,
      Field[] keys,
      Object... values) {
    return new DefaultRecord(source, resource, position, keys, values);
  }

  /**
   * Creates a mapped record with the given map of keys and values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param values the record keys and values.
   * @return a mapped record.
   */
  public static DefaultRecord mapped(
      @NonNull Object source,
      @NonNull URI resource,
      long position,
      Map<? extends Field, ?> values) {
    return new DefaultRecord(source, resource, position, values);
  }

  private final Object source;
  private final Supplier<URI> resource;
  private final long position;

  /**
   * Creates an empty record.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   */
  public DefaultRecord(Object source, URI resource, long position) {
    this.source = source;
    this.resource = () -> resource;
    this.position = position;
  }

  /**
   * Creates an empty record.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc); the supplier
   *     will be memoized.
   * @param position the record position inside the resource (line number, etc.).
   */
  public DefaultRecord(Object source, Supplier<URI> resource, long position) {
    this.source = source;
    this.resource = Suppliers.memoize(resource::get);
    this.position = position;
  }

  private DefaultRecord(Object source, URI resource, long position, Object... values) {
    this.source = source;
    this.resource = () -> resource;
    this.position = position;
    Streams.forEachPair(
        IntStream.range(0, values.length).boxed().map(DefaultIndexedField::new),
        Arrays.stream(values),
        this::put);
  }

  private DefaultRecord(
      Object source, URI resource, long position, Field[] keys, Object... values) {
    this.resource = () -> resource;
    this.position = position;
    if (keys.length != values.length) {
      throw new IllegalArgumentException(
          String.format(
              "Expecting record to contain %d fields but found %d.", keys.length, values.length));
    }
    this.source = source;
    Streams.forEachPair(Arrays.stream(keys), Arrays.stream(values), this::put);
  }

  private DefaultRecord(
      Object source, URI resource, long position, Map<? extends Field, ?> values) {
    this.resource = () -> resource;
    this.position = position;
    this.source = source;
    putAll(values);
  }

  @NonNull
  @Override
  public Object getSource() {
    return source;
  }

  @NonNull
  @Override
  public URI getResource() {
    return resource.get();
  }

  @Override
  public long getPosition() {
    return position;
  }

  @NonNull
  @Override
  public Set<Field> fields() {
    return keySet();
  }

  @NonNull
  @Override
  public Collection<Object> values() {
    return super.values();
  }

  @Override
  public Object getFieldValue(@NonNull Field field) {
    return get(field);
  }

  /**
   * Sets the value associated with the given field.
   *
   * @param field the field to set.
   * @param value The value to set.
   */
  public void setFieldValue(@NonNull Field field, Object value) {
    put(field, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", source)
        .add("entries", entrySet())
        .toString();
  }
}
