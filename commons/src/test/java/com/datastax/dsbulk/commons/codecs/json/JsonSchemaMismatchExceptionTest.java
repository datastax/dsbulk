/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

class JsonSchemaMismatchExceptionTest {

  @Test
  void should_report_object_with_extraneous_and_missing_fields() {
    assertThat(
            JsonSchemaMismatchException.objectHasMissingAndExtraneousFields(
                Sets.newLinkedHashSet("extra1"), Sets.newLinkedHashSet("missing1")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 1 extraneous field: 'extra1' and 1 missing field: 'missing1' "
                + "(set schema.allowExtraFields to true to allow JSON objects to contain fields not present "
                + "in the UDT definition and set schema.allowMissingFields to true to allow JSON objects "
                + "to lack of fields present in the UDT definition).");
    assertThat(
            JsonSchemaMismatchException.objectHasMissingAndExtraneousFields(
                Sets.newLinkedHashSet("extra1", "extra2"),
                Sets.newLinkedHashSet("missing1", "missing2")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 2 extraneous fields: 'extra1', 'extra2' and 2 missing fields: 'missing1', 'missing2' "
                + "(set schema.allowExtraFields to true to allow JSON objects to contain fields not present "
                + "in the UDT definition and set schema.allowMissingFields to true to allow JSON objects "
                + "to lack of fields present in the UDT definition).");
  }

  @Test
  void should_report_object_with_extraneous_fields() {
    assertThat(
            JsonSchemaMismatchException.objectHasExtraneousFields(Sets.newLinkedHashSet("extra1")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 1 extraneous field: 'extra1' "
                + "(set schema.allowExtraFields to true to allow JSON objects to contain fields not present "
                + "in the UDT definition).");
    assertThat(
            JsonSchemaMismatchException.objectHasExtraneousFields(
                Sets.newLinkedHashSet("extra1", "extra2")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 2 extraneous fields: 'extra1', 'extra2' "
                + "(set schema.allowExtraFields to true to allow JSON objects to contain fields not present "
                + "in the UDT definition).");
  }

  @Test
  void should_report_object_with_missing_fields() {
    assertThat(
            JsonSchemaMismatchException.objectHasMissingFields(Sets.newLinkedHashSet("missing1")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 1 missing field: 'missing1' "
                + "(set schema.allowMissingFields to true to allow JSON objects "
                + "to lack of fields present in the UDT definition).");
    assertThat(
            JsonSchemaMismatchException.objectHasMissingFields(
                Sets.newLinkedHashSet("missing1", "missing2")))
        .hasMessage(
            "JSON object does not match UDT definition: "
                + "found 2 missing fields: 'missing1', 'missing2' "
                + "(set schema.allowMissingFields to true to allow JSON objects "
                + "to lack of fields present in the UDT definition).");
  }

  @Test
  void should_report_array_size_greater_than_udt_size() {
    assertThat(JsonSchemaMismatchException.arraySizeGreaterThanUDTSize(3, 4))
        .hasMessage(
            "JSON array does not match UDT definition: expecting 3 elements, got 4 "
                + "(set schema.allowExtraFields to true to allow JSON arrays to contain "
                + "more elements than the UDT definition).");
  }

  @Test
  void should_report_array_size_lesser_than_udt_size() {
    assertThat(JsonSchemaMismatchException.arraySizeLesserThanUDTSize(3, 2))
        .hasMessage(
            "JSON array does not match UDT definition: expecting 3 elements, got 2 "
                + "(set schema.allowMissingFields to true to allow JSON arrays to contain "
                + "fewer elements than the UDT definition).");
  }

  @Test
  void should_report_array_size_greater_than_tuple_size() {
    assertThat(JsonSchemaMismatchException.arraySizeGreaterThanTupleSize(3, 4))
        .hasMessage(
            "JSON array does not match tuple definition: expecting 3 elements, got 4 "
                + "(set schema.allowExtraFields to true to allow JSON arrays to contain "
                + "more elements than the tuple definition).");
  }

  @Test
  void should_report_array_size_lesser_than_tuple_size() {
    assertThat(JsonSchemaMismatchException.arraySizeLesserThanTupleSize(3, 2))
        .hasMessage(
            "JSON array does not match tuple definition: expecting 3 elements, got 2 "
                + "(set schema.allowMissingFields to true to allow JSON arrays to contain "
                + "fewer elements than the tuple definition).");
  }
}
