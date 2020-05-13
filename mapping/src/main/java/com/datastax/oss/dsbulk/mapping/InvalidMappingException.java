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
package com.datastax.oss.dsbulk.mapping;

import static com.datastax.oss.dsbulk.mapping.CQLRenderMode.VARIABLE;

import com.datastax.oss.dsbulk.connectors.api.Field;
import edu.umd.cs.findbugs.annotations.NonNull;

public class InvalidMappingException extends RuntimeException {

  private InvalidMappingException(String msg) {
    super(msg);
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
      @NonNull Field field, @NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Required field "
            + field.getFieldDescription()
            + " (mapped to column "
            + variable.render(VARIABLE)
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
  public static InvalidMappingException emptyPrimaryKey(@NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be set to empty. "
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
}
