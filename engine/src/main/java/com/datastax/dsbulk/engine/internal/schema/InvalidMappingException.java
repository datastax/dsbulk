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
import edu.umd.cs.findbugs.annotations.NonNull;

public class InvalidMappingException extends RuntimeException {

  private InvalidMappingException(String msg) {
    super(msg);
  }

  @NonNull
  static InvalidMappingException extraneousField(@NonNull Field field) {
    return new InvalidMappingException(
        "Extraneous field "
            + field.getFieldDescription()
            + " was found in record. "
            + "Please declare it explicitly in the mapping "
            + "or set schema.allowExtraFields to true.");
  }

  @NonNull
  static InvalidMappingException missingField(@NonNull Field field, @NonNull CQLWord variable) {
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
  static InvalidMappingException nullPrimaryKey(@NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be mapped to null. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }

  @NonNull
  static InvalidMappingException unsetPrimaryKey(@NonNull CQLWord variable) {
    return new InvalidMappingException(
        "Primary key column "
            + variable.render(VARIABLE)
            + " cannot be left unmapped. "
            + "Check that your settings (schema.mapping or schema.query) match your dataset contents.");
  }
}
