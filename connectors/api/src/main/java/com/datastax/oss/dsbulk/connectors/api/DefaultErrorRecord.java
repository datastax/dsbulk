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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Set;

/** A record that could not be fully parsed from its source. */
public class DefaultErrorRecord implements ErrorRecord {

  private final @Nullable Object source;
  private final @NonNull URI resource;
  private final long position;
  private final @NonNull Throwable error;

  /**
   * Creates a new error record.
   *
   * @param source the record's source; may be null if the source cannot be determined or should not
   *     be retained.
   * @param resource the record's resource.
   * @param position the record's position.
   * @param error the error.
   */
  public DefaultErrorRecord(
      @Nullable Object source, @NonNull URI resource, long position, @NonNull Throwable error) {
    this.source = source;
    this.resource = resource;
    this.position = position;
    this.error = error;
  }

  @Nullable
  @Override
  public Object getSource() {
    return source;
  }

  @NonNull
  @Override
  public URI getResource() {
    return resource;
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
