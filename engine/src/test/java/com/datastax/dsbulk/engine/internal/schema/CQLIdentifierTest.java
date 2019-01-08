/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.ALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.INTERNAL;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.POSITIONAL_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.UNALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CQLIdentifierTest {

  private static final CQLIdentifier FOO = CQLIdentifier.fromInternal("foo");
  private static final CQLIdentifier FOO_UP = CQLIdentifier.fromInternal("Foo");
  private static final CQLIdentifier FOO_WS = CQLIdentifier.fromInternal("foo bar");
  private static final CQLIdentifier FOO_QUOTE = CQLIdentifier.fromInternal("foo\"bar");
  private static final CQLIdentifier FOO_KW = CQLIdentifier.fromInternal("create");

  @ParameterizedTest
  @MethodSource
  void should_build_from_internal(String internal, CQLIdentifier expected) {
    assertThat(CQLIdentifier.fromInternal(internal)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_build_from_internal() {
    return Lists.newArrayList(
        arguments("foo", FOO),
        arguments("Foo", FOO_UP),
        arguments("foo bar", FOO_WS),
        arguments("foo\"bar", FOO_QUOTE),
        arguments("create", FOO_KW));
  }

  @ParameterizedTest
  @MethodSource
  void should_build_from_valid_cql(String cql, CQLIdentifier expected) {
    assertThat(CQLIdentifier.fromCql(cql)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_build_from_valid_cql() {
    return Lists.newArrayList(
        arguments("foo", FOO),
        arguments("Foo", FOO),
        arguments("\"Foo\"", FOO_UP),
        arguments("\"foo bar\"", FOO_WS),
        arguments("\"foo\"\"bar\"", FOO_QUOTE),
        arguments("\"create\"", FOO_KW));
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

  @ParameterizedTest
  @MethodSource
  void should_render_identifier(CQLIdentifier identifier, CQLRenderMode mode, String expected) {
    assertThat(identifier.render(mode)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_render_identifier() {
    return Lists.newArrayList(
        // INTERNAL
        arguments(FOO, INTERNAL, "foo"),
        arguments(FOO_UP, INTERNAL, "Foo"),
        arguments(FOO_WS, INTERNAL, "foo bar"),
        arguments(FOO_QUOTE, INTERNAL, "foo\"bar"),
        arguments(FOO_KW, INTERNAL, "create"),
        // VARIABLE
        arguments(FOO, VARIABLE, "foo"),
        arguments(FOO_UP, VARIABLE, "\"Foo\""),
        arguments(FOO_WS, VARIABLE, "\"foo bar\""),
        arguments(FOO_QUOTE, VARIABLE, "\"foo\"\"bar\""),
        arguments(FOO_KW, VARIABLE, "\"create\""),
        // NAMED_ASSIGNMENT
        arguments(FOO, NAMED_ASSIGNMENT, ":foo"),
        arguments(FOO_UP, NAMED_ASSIGNMENT, ":\"Foo\""),
        arguments(FOO_WS, NAMED_ASSIGNMENT, ":\"foo bar\""),
        arguments(FOO_QUOTE, NAMED_ASSIGNMENT, ":\"foo\"\"bar\""),
        arguments(FOO_KW, NAMED_ASSIGNMENT, ":\"create\""),
        // POSITIONAL_ASSIGNMENT
        arguments(FOO, POSITIONAL_ASSIGNMENT, "?"),
        arguments(FOO_UP, POSITIONAL_ASSIGNMENT, "?"),
        arguments(FOO_WS, POSITIONAL_ASSIGNMENT, "?"),
        arguments(FOO_QUOTE, POSITIONAL_ASSIGNMENT, "?"),
        arguments(FOO_KW, POSITIONAL_ASSIGNMENT, "?"),
        // UNALIASED_SELECTOR
        arguments(FOO, UNALIASED_SELECTOR, "foo"),
        arguments(FOO_UP, UNALIASED_SELECTOR, "\"Foo\""),
        arguments(FOO_WS, UNALIASED_SELECTOR, "\"foo bar\""),
        arguments(FOO_QUOTE, UNALIASED_SELECTOR, "\"foo\"\"bar\""),
        arguments(FOO_KW, UNALIASED_SELECTOR, "\"create\""),
        // ALIASED_SELECTOR
        arguments(FOO, ALIASED_SELECTOR, "foo"),
        arguments(FOO_UP, ALIASED_SELECTOR, "\"Foo\""),
        arguments(FOO_WS, ALIASED_SELECTOR, "\"foo bar\""),
        arguments(FOO_QUOTE, ALIASED_SELECTOR, "\"foo\"\"bar\""),
        arguments(FOO_KW, ALIASED_SELECTOR, "\"create\""));
  }
}
