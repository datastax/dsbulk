/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.oss.driver.shaded.guava.common.base.Suppliers;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/** A record that could not be fully parsed from its source. */
public class DefaultErrorRecord implements ErrorRecord {

  private final @NonNull Object source;
  private final @NonNull Supplier<URI> resource;
  private final long position;
  private final @NonNull Throwable error;

  /**
   * Creates a new error record.
   *
   * @param source the record's source.
   * @param resource the record's resource.
   * @param position the record's position.
   * @param error the error.
   */
  public DefaultErrorRecord(
      @NonNull Object source, @NonNull URI resource, long position, @NonNull Throwable error) {
    this(source, () -> resource, position, error);
  }

  /**
   * Creates a new error record.
   *
   * @param source the record's source.
   * @param resource the record's resource; will be memoized.
   * @param position the record's position.
   * @param error the error.
   */
  public DefaultErrorRecord(
      @NonNull Object source,
      @NonNull Supplier<URI> resource,
      long position,
      @NonNull Throwable error) {
    this.source = source;
    this.resource = Suppliers.memoize(resource::get);
    this.position = position;
    this.error = error;
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
    return ImmutableSet.of();
  }

  @NonNull
  @Override
  public Collection<Object> values() {
    return ImmutableList.of();
  }

  @Override
  public Object getFieldValue(@NonNull Field field) {
    return null;
  }

  @Override
  public void clear() {
    // NO-OP
  }

  @NonNull
  @Override
  public Throwable getError() {
    return error;
  }
}
