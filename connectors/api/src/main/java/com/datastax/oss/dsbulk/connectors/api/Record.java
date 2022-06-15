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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * An item read by a {@link Connector}, or passed to a connector for writing. A record is composed
 * of one or more {@linkplain Field fields}.
 *
 * <p>Records typically originate from a line in a file, or a row in a database table.
 */
public interface Record extends Trackable {

  /**
   * Returns a set containing all the fields in this record.
   *
   * @return a set containing all the fields in this record.
   */
  @NonNull
  Set<Field> fields();

  /**
   * Returns a collection containing all the values in this record.
   *
   * <p>The iteration order of this collection should match that of {@link #fields()}.
   *
   * @return a collection containing all the values in this record.
   */
  @NonNull
  Collection<Object> values();

  /**
   * Returns the value associated with the given field.
   *
   * <p>Note that a return value of {@code null} may indicate that the record contains no such
   * field, or that the field value was {@code null}.
   *
   * @param field the field.
   * @return the value associated with the given field.
   */
  @Nullable
  Object getFieldValue(@NonNull Field field);

  /**
   * Clear all fields in this record.
   *
   * <p>This method should be used to free memory, when this record's fields have been successfully
   * processed and are no longer required.
   */
  void clear();
}
