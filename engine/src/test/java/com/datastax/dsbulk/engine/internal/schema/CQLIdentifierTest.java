/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CQLIdentifierTest {

  @Test
  void should_build_from_internal() {
    assertThat(CQLIdentifier.fromInternal("foo").asInternal()).isEqualTo("foo");
    assertThat(CQLIdentifier.fromInternal("Foo").asInternal()).isEqualTo("Foo");
    assertThat(CQLIdentifier.fromInternal("foo bar").asInternal()).isEqualTo("foo bar");
    assertThat(CQLIdentifier.fromInternal("foo\"bar").asInternal()).isEqualTo("foo\"bar");
    assertThat(CQLIdentifier.fromInternal("create").asInternal()).isEqualTo("create");
  }

  @Test
  void should_build_from_valid_cql() {
    assertThat(CQLIdentifier.fromCql("foo").asInternal()).isEqualTo("foo");
    assertThat(CQLIdentifier.fromCql("Foo").asInternal()).isEqualTo("foo");
    assertThat(CQLIdentifier.fromCql("\"Foo\"").asInternal()).isEqualTo("Foo");
    assertThat(CQLIdentifier.fromCql("\"foo bar\"").asInternal()).isEqualTo("foo bar");
    assertThat(CQLIdentifier.fromCql("\"foo\"\"bar\"").asInternal()).isEqualTo("foo\"bar");
    assertThat(CQLIdentifier.fromCql("\"create\"").asInternal()).isEqualTo("create");
  }

  @Test
  void should_fail_to_build_from_valid_cql_if_special_characters() {
    assertThatThrownBy(() -> CQLIdentifier.fromCql("foo bar"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CQL form [foo bar]: needs double quotes");
  }

  @Test
  void should_fail_to_build_from_valid_cql_if_reserved_keyword() {
    assertThatThrownBy(() -> CQLIdentifier.fromCql("Create"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CQL form [Create]: needs double quotes");
  }

  @Test
  void should_format_as_cql() {
    assertThat(CQLIdentifier.fromInternal("foo").asCql()).isEqualTo("foo");
    assertThat(CQLIdentifier.fromInternal("Foo").asCql()).isEqualTo("\"Foo\"");
    assertThat(CQLIdentifier.fromInternal("foo bar").asCql()).isEqualTo("\"foo bar\"");
    assertThat(CQLIdentifier.fromInternal("foo\"bar").asCql()).isEqualTo("\"foo\"\"bar\"");
    assertThat(CQLIdentifier.fromInternal("create").asCql()).isEqualTo("\"create\"");
  }
}
