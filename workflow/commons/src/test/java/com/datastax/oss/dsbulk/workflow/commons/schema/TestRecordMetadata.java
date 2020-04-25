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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TestRecordMetadata implements RecordMetadata {

  private final ImmutableMap<Field, GenericType<?>> fieldsToTypes;

  TestRecordMetadata(ImmutableMap<Field, GenericType<?>> fieldsToTypes) {
    this.fieldsToTypes = fieldsToTypes;
  }

  @NonNull
  @Override
  public GenericType<?> getFieldType(@NonNull Field field, @NonNull DataType cqlType) {
    return fieldsToTypes.get(field);
  }
}
