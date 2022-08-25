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
package com.datastax.oss.dsbulk.workflow.commons.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RangeTest {

  @ParameterizedTest
  @MethodSource
  void should_parse(String text, Range expected) {
    assertThat(Range.parse(text)).isEqualTo(expected);
  }

  static Stream<Arguments> should_parse() {
    return Stream.of(
        Arguments.of("1:10", new Range(1, 10)),
        Arguments.of("0:0", new Range(0)),
        Arguments.of("123", new Range(123)),
        Arguments.of("-1", new Range(-1)),
        Arguments.of("-10:-1", new Range(-10, -1)));
  }

  @ParameterizedTest
  @MethodSource
  void should_serialize_as_text(Range range, String expected) {
    assertThat(range.asText()).isEqualTo(expected);
  }

  static Stream<Arguments> should_serialize_as_text() {
    return Stream.of(
        Arguments.of(new Range(1, 10), "1:10"),
        Arguments.of(new Range(0), "0"),
        Arguments.of(new Range(123), "123"),
        Arguments.of(new Range(-1, -1), "-1"),
        Arguments.of(new Range(-10, -1), "-10:-1"));
  }

  @ParameterizedTest
  @MethodSource
  void should_is_contiguous(Range range, Range other, boolean expected) {
    assertThat(range.isContiguous(other)).isEqualTo(expected);
  }

  static Stream<Arguments> should_is_contiguous() {
    return Stream.of(
        Arguments.of(new Range(4, 6), new Range(8, 10), false),
        Arguments.of(new Range(4, 6), new Range(8), false),
        Arguments.of(new Range(4, 6), new Range(7, 8), true),
        Arguments.of(new Range(4, 6), new Range(7), true),
        Arguments.of(new Range(4, 6), new Range(6, 8), true),
        Arguments.of(new Range(4, 6), new Range(5, 7), true),
        Arguments.of(new Range(4, 6), new Range(4, 7), true),
        Arguments.of(new Range(4, 6), new Range(4, 6), true),
        Arguments.of(new Range(4, 6), new Range(3, 7), true),
        Arguments.of(new Range(4, 6), new Range(3, 6), true),
        Arguments.of(new Range(4, 6), new Range(6), true),
        Arguments.of(new Range(4, 6), new Range(5), true),
        Arguments.of(new Range(4, 6), new Range(4), true),
        Arguments.of(new Range(4, 6), new Range(3, 5), true),
        Arguments.of(new Range(4, 6), new Range(2, 4), true),
        Arguments.of(new Range(4, 6), new Range(3), true),
        Arguments.of(new Range(4, 6), new Range(2, 3), true),
        Arguments.of(new Range(4, 6), new Range(2), false),
        Arguments.of(new Range(4, 6), new Range(1, 2), false));
  }

  @ParameterizedTest
  @MethodSource
  void should_merge_range(Range range, Range other, Range expected) {
    range.merge(other);
    assertThat(range).isEqualTo(expected);
  }

  static Stream<Arguments> should_merge_range() {
    return Stream.of(
        Arguments.of(new Range(4, 5), new Range(1, 2), new Range(1, 5)),
        Arguments.of(new Range(4, 5), new Range(2, 3), new Range(2, 5)),
        Arguments.of(new Range(4, 5), new Range(3, 5), new Range(3, 5)),
        Arguments.of(new Range(4, 5), new Range(4, 5), new Range(4, 5)),
        Arguments.of(new Range(4, 5), new Range(4, 6), new Range(4, 6)),
        Arguments.of(new Range(4, 5), new Range(4, 7), new Range(4, 7)),
        Arguments.of(new Range(4, 5), new Range(5, 7), new Range(4, 7)),
        Arguments.of(new Range(4, 5), new Range(6, 7), new Range(4, 7)),
        Arguments.of(new Range(4, 5), new Range(1, 10), new Range(1, 10)),
        Arguments.of(new Range(1, 10), new Range(4, 5), new Range(1, 10)));
  }
}
