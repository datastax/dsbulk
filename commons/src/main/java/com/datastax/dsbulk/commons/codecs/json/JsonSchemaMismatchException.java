/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Set;

class JsonSchemaMismatchException extends IllegalArgumentException {

  private JsonSchemaMismatchException(String message) {
    super(message);
  }

  @NonNull
  static JsonSchemaMismatchException objectHasMissingAndExtraneousFields(
      Set<String> extraneous, Set<String> missing) {
    StringBuilder msg =
        new StringBuilder("JSON object does not match UDT definition: found ")
            .append(extraneous.size())
            .append(" extraneous field");
    if (extraneous.size() > 1) {
      msg.append('s');
    }
    msg.append(": '")
        .append(String.join("', '", extraneous))
        .append("' and ")
        .append(missing.size())
        .append(" missing field");
    if (missing.size() > 1) {
      msg.append('s');
    }
    msg.append(": '")
        .append(String.join("', '", missing))
        .append("' (set schema.allowExtraFields to true to allow ")
        .append("JSON objects to contain fields not present in the UDT definition and ")
        .append("set schema.allowMissingFields to true to allow ")
        .append("JSON objects to lack of fields present in the UDT definition).");
    return new JsonSchemaMismatchException(msg.toString());
  }

  @NonNull
  static JsonSchemaMismatchException objectHasExtraneousFields(Set<String> extraneous) {
    StringBuilder msg =
        new StringBuilder("JSON object does not match UDT definition: found ")
            .append(extraneous.size())
            .append(" extraneous field");
    if (extraneous.size() > 1) {
      msg.append('s');
    }
    msg.append(": '")
        .append(String.join("', '", extraneous))
        .append("' (set schema.allowExtraFields to true to allow ")
        .append("JSON objects to contain fields not present in the UDT definition).");
    return new JsonSchemaMismatchException(msg.toString());
  }

  @NonNull
  static JsonSchemaMismatchException objectHasMissingFields(Set<String> missing) {
    StringBuilder msg =
        new StringBuilder("JSON object does not match UDT definition: found ")
            .append(missing.size())
            .append(" missing field");
    if (missing.size() > 1) {
      msg.append('s');
    }
    msg.append(": '")
        .append(String.join("', '", missing))
        .append("' (set schema.allowMissingFields to true to allow ")
        .append("JSON objects to lack of fields present in the UDT definition).");
    return new JsonSchemaMismatchException(msg.toString());
  }

  @NonNull
  static JsonSchemaMismatchException arraySizeGreaterThanUDTSize(int udtSize, int nodeSize) {
    return new JsonSchemaMismatchException(
        String.format(
            "JSON array does not match UDT definition: expecting %d elements, got %d "
                + "(set schema.allowExtraFields to true to allow "
                + "JSON arrays to contain more elements than the UDT definition).",
            udtSize, nodeSize));
  }

  @NonNull
  static JsonSchemaMismatchException arraySizeLesserThanUDTSize(int udtSize, int nodeSize) {
    return new JsonSchemaMismatchException(
        String.format(
            "JSON array does not match UDT definition: expecting %d elements, got %d "
                + "(set schema.allowMissingFields to true to allow "
                + "JSON arrays to contain fewer elements than the UDT definition).",
            udtSize, nodeSize));
  }

  @NonNull
  static JsonSchemaMismatchException arraySizeLesserThanTupleSize(int tupleSize, int nodeSize) {
    return new JsonSchemaMismatchException(
        String.format(
            "JSON array does not match tuple definition: expecting %d elements, got %d "
                + "(set schema.allowMissingFields to true to allow "
                + "JSON arrays to contain fewer elements than the tuple definition).",
            tupleSize, nodeSize));
  }

  @NonNull
  static JsonSchemaMismatchException arraySizeGreaterThanTupleSize(int tupleSize, int nodeSize) {
    return new JsonSchemaMismatchException(
        String.format(
            "JSON array does not match tuple definition: expecting %d elements, got %d "
                + "(set schema.allowExtraFields to true to allow "
                + "JSON arrays to contain more elements than the tuple definition).",
            tupleSize, nodeSize));
  }
}
