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

import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.VARIABLE;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Set;
import java.util.stream.Collectors;

public class InvalidMappingException extends RuntimeException {

  private InvalidMappingException(String msg) {
    super(msg);
  }

  private InvalidMappingException(String msg, Throwable cause) {
    super(msg, cause);
  }

  @NonNull
  public static InvalidMappingException extraneousField(@NonNull Field field) {
    return new InvalidMappingException(
        "Extraneous field "
            + field.getFieldDescription()
            + " was found in record. "
            + "Please declare it explicitly in the mapping "
            + "or set schema.allowExtraFields to true.");
  }

  @NonNull
  public static InvalidMappingException missingField(
      @NonNull Field field, @NonNull Set<CQLWord> variables) {
    return new InvalidMappingException(
        "Required field "
            + field.getFieldDescription()
            + " (mapped to column"
            + (variables.size() > 1 ? "s " : " ")
            + variables.stream().map(v -> v.render(VARIABLE)).collect(Collectors.joining(", "))
            + ") was missing from record. "
            + "Please remove it from the mapping "
            + "or set schema.allowMissingFields to true.");
  }

  @NonNull
  public static InvalidMappingException nullPrimaryKey(@NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be set to null. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }

  @NonNull
  public static InvalidMappingException unsetPrimaryKey(@NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be left unset. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }

  @NonNull
  public static InvalidMappingException encodeFailed(
      @NonNull Field field,
      @NonNull CQLWord variable,
      @NonNull GenericType<?> fieldType,
      @NonNull DataType cqlType,
      @Nullable Object raw,
      @NonNull Exception cause) {
    return new InvalidMappingException(
        String.format(
            "Could not map field %s to variable %s; conversion from Java type %s to CQL type %s failed for raw value: %s.",
            field.getFieldDescription(), variable.render(VARIABLE), fieldType, cqlType, raw),
        cause);
  }
}
