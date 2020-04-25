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
package com.datastax.oss.dsbulk.codecs.text.json;

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
