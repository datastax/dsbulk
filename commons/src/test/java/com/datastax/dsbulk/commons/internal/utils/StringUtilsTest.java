/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StringUtilsTest {

  @ParameterizedTest
  @MethodSource
  void should_ensure_brackets(String input, String expected) {
    assertThat(StringUtils.ensureBrackets(input)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_ensure_brackets() {
    return Stream.of(
        Arguments.of("", "[]"),
        Arguments.of("[]", "[]"),
        Arguments.of("foo", "[foo]"),
        Arguments.of("[foo]", "[foo]"));
  }

  @ParameterizedTest
  @MethodSource
  void should_ensure_braces(String input, String expected) {
    assertThat(StringUtils.ensureBraces(input)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_ensure_braces() {
    return Stream.of(
        Arguments.of("", "{}"),
        Arguments.of("{}", "{}"),
        Arguments.of("foo", "{foo}"),
        Arguments.of("{foo}", "{foo}"));
  }

  @ParameterizedTest
  @MethodSource
  void should_ensure_quoted(String input, String expected) {
    assertThat(StringUtils.ensureQuoted(input)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_ensure_quoted() {
    return Stream.of(
        Arguments.of("", "\"\""),
        Arguments.of("\"", "\""),
        Arguments.of("\\\"", "\"\\\"\""),
        Arguments.of("\"\"", "\"\""),
        Arguments.of("foo", "\"foo\""),
        Arguments.of("\"foo\"", "\"foo\""));
  }

  @ParameterizedTest
  @MethodSource
  void should_quote_jmx_if_necessary(String input, String expected) {
    assertThat(StringUtils.quoteJMXIfNecessary(input)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_quote_jmx_if_necessary() {
    return Stream.of(
        Arguments.of("", "\"\""),
        Arguments.of("foo", "foo"),
        Arguments.of("foo-bar", "foo-bar"),
        Arguments.of("foo_bar", "foo_bar"),
        Arguments.of("foo?", "\"foo\\?\""),
        Arguments.of("foo*", "\"foo\\*\""),
        Arguments.of("foo\\", "\"foo\\\\\""),
        Arguments.of("foo\n", "\"foo\\n\""),
        Arguments.of("foo bar", "\"foo bar\""),
        Arguments.of("foo|bar", "\"foo|bar\""),
        Arguments.of("foo,bar", "\"foo,bar\""));
  }

  @Test
  void should_format_elapsed_time() {
    assertThat(StringUtils.formatElapsed(12)).isEqualTo("12 seconds");
    assertThat(StringUtils.formatElapsed(12 * 60 + 34)).isEqualTo("12 minutes and 34 seconds");
    assertThat(StringUtils.formatElapsed(12 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("12 hours, 34 minutes and 56 seconds");
    assertThat(StringUtils.formatElapsed(48 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("48 hours, 34 minutes and 56 seconds");
  }
}
