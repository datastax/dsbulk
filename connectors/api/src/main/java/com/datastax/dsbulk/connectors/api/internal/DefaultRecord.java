/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Streams;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;

public class DefaultRecord extends LinkedHashMap<Field, Object> implements Record {

  /**
   * Creates an indexed record with the given values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param location the record location, usually its resource combined with its position.
   * @param values the record values.
   * @return an indexed record.
   */
  public static DefaultRecord indexed(
      @NotNull Object source,
      @NotNull Supplier<URI> resource,
      long position,
      @NotNull Supplier<URI> location,
      Object... values) {
    return new DefaultRecord(source, resource, position, location, values);
  }

  /**
   * Creates a mapped record with the given keys and values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param location the record location, usually its resource combined with its position.
   * @param keys the record keys.
   * @param values the record values.
   * @return a mapped record.
   */
  public static DefaultRecord mapped(
      @NotNull Object source,
      @NotNull Supplier<URI> resource,
      long position,
      @NotNull Supplier<URI> location,
      Field[] keys,
      Object... values) {
    return new DefaultRecord(source, resource, position, location, keys, values);
  }

  /**
   * Creates a mapped record with the given map of keys and values.
   *
   * @param source the record source (its original form).
   * @param resource the record resource (where it comes from: file, database, etc).
   * @param position the record position inside the resource (line number, etc.).
   * @param location the record location, usually its resource combined with its position.
   * @param values the record keys and values.
   * @return a mapped record.
   */
  public static DefaultRecord mapped(
      @NotNull Object source,
      @NotNull Supplier<URI> resource,
      long position,
      @NotNull Supplier<URI> location,
      Map<? extends Field, ?> values) {
    return new DefaultRecord(source, resource, position, location, values);
  }

  private final Object source;
  private final Supplier<URI> resource;
  private final long position;
  private final Supplier<URI> location;

  private DefaultRecord(
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
        IntStream.range(0, values.length).boxed().map(DefaultIndexedField::new),
        Arrays.stream(values),
        this::put);
  }

  private DefaultRecord(
      Object source,
      Supplier<URI> resource,
      long position,
      Supplier<URI> location,
      Field[] keys,
      Object... values) {
    this.resource = resource;
    this.position = position;
    if (keys.length != values.length) {
      throw new IllegalArgumentException(
          String.format(
              "Expecting record to contain %d fields but found %d.", keys.length, values.length));
    }
    this.source = source;
    this.location = location;
    Streams.forEachPair(Arrays.stream(keys), Arrays.stream(values), this::put);
  }

  private DefaultRecord(
      Object source,
      Supplier<URI> resource,
      long position,
      Supplier<URI> location,
      Map<? extends Field, ?> values) {
    this.resource = resource;
    this.position = position;
    this.source = source;
    this.location = location;
    putAll(values);
  }

  @NotNull
  @Override
  public Object getSource() {
    return source;
  }

  @NotNull
  @Override
  public URI getResource() {
    return resource.get();
  }

  @Override
  public long getPosition() {
    return position;
  }

  @NotNull
  @Override
  public URI getLocation() {
    return location.get();
  }

  @NotNull
  @Override
  public Set<Field> fields() {
    return keySet();
  }

  @NotNull
  @Override
  public Collection<Object> values() {
    return super.values();
  }

  @Override
  public Object getFieldValue(@NotNull Field field) {
    return get(field);
  }

  /**
   * Sets the value associated with the given field.
   *
   * @param field the field to set.
   * @param value The value to set.
   */
  public void setFieldValue(@NotNull Field field, Object value) {
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
