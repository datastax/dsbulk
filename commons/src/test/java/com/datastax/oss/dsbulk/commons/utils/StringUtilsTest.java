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
package com.datastax.oss.dsbulk.commons.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
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
}
