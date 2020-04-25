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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Defines metadata applicable to a {@link Record record}, in particular which field types it
 * contains.
 */
public interface RecordMetadata {

  /**
   * Returns the type of the given field.
   *
   * @param field the field to get the type from.
   * @param cqlType the CQL type associated with the given field.
   * @return the type of the given field.
   */
  @NonNull
  GenericType<?> getFieldType(@NonNull Field field, @NonNull DataType cqlType);
}
