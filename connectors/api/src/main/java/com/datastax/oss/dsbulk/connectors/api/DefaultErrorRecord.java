/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.connectors.api;

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
