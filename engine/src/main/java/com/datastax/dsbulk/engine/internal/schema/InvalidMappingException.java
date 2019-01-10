/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;

import com.datastax.dsbulk.connectors.api.Field;
import org.jetbrains.annotations.NotNull;

public class InvalidMappingException extends RuntimeException {

  private InvalidMappingException(String msg) {
    super(msg);
  }

  @NotNull
  static InvalidMappingException extraneousField(@NotNull Field field) {
    return new InvalidMappingException(
        "Extraneous field "
            + field.getFieldDescription()
            + " was found in record. "
            + "Please declare it explicitly in the mapping "
            + "or set schema.allowExtraFields to true.");
  }

  @NotNull
  static InvalidMappingException missingField(
      @NotNull Field field, @NotNull CQLIdentifier variable) {
    return new InvalidMappingException(
        "Required field "
            + field.getFieldDescription()
            + " (mapped to column "
            + variable.render(VARIABLE)
            + ") was missing from record. "
            + "Please remove it from the mapping "
            + "or set schema.allowMissingFields to true.");
  }

  @NotNull
  static InvalidMappingException nullPrimaryKey(@NotNull CQLIdentifier variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be mapped to null. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }

  @NotNull
  static InvalidMappingException unsetPrimaryKey(@NotNull CQLIdentifier variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be left unmapped. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }
}
