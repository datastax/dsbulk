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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RangeSetTest {

  @ParameterizedTest
  @MethodSource
  void should_parse(String text, RangeSet expected) {
    RangeSet rs = RangeSet.parse(text);
    assertThat(rs).isEqualTo(expected);
  }

  static Stream<Arguments> should_parse() {
    return Stream.of(
        Arguments.of("", RangeSet.of()),
        Arguments.of("1:10", RangeSet.of(new Range(1, 10))),
        Arguments.of(
            "1:10,12:20,30:30", RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30))));
  }

  @ParameterizedTest
  @MethodSource
  void should_serialize_as_text(RangeSet rs, String expected) {
    assertThat(rs.asText()).isEqualTo(expected);
  }

  static Stream<Arguments> should_serialize_as_text() {
    return Stream.of(
        Arguments.of(RangeSet.of(), ""),
        Arguments.of(RangeSet.of(new Range(1, 10)), "1:10"),
        Arguments.of(
            RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30)), "1:10,12:20,30"));
  }

  @ParameterizedTest
  @MethodSource
  void should_contain(RangeSet rs, long position, boolean expected) {
    assertThat(rs.contains(position)).isEqualTo(expected);
  }

  static Stream<Arguments> should_contain() {
    // Note: more test cases are covered in RangeUtilsTest.
    return Stream.of(
        Arguments.of(RangeSet.of(), 0, false),
        Arguments.of(RangeSet.of(new Range(0, 0)), 0, true),
        Arguments.of(RangeSet.of(new Range(1, 1)), 0, false),
        Arguments.of(RangeSet.of(new Range(0, 0)), 1, false),
        Arguments.of(RangeSet.of(new Range(0, 0), new Range(2, 2)), 1, false),
        Arguments.of(RangeSet.of(new Range(0, 0), new Range(2, 2)), 2, true));
  }

  @ParameterizedTest
  @MethodSource
  void should_update(RangeSet rs, long position, Range... expected) {
    rs.update(position);
    assertThat(rs.stream()).containsExactly(expected);
  }

  static Stream<Arguments> should_update() {
    // Note: more test cases are covered in RangeUtilsTest.
    return Stream.of(
        Arguments.of(RangeSet.of(), 0, new Range[] {new Range(0, 0)}),
        Arguments.of(RangeSet.of(new Range(0, 0)), 0, new Range[] {new Range(0, 0)}),
        Arguments.of(RangeSet.of(new Range(1, 2)), 0, new Range[] {new Range(0, 2)}),
        Arguments.of(RangeSet.of(new Range(1, 2)), 3, new Range[] {new Range(1, 3)}),
        Arguments.of(
            RangeSet.of(new Range(2, 3)), 0, new Range[] {new Range(0, 0), new Range(2, 3)}),
        Arguments.of(
            RangeSet.of(new Range(0, 1)), 3, new Range[] {new Range(0, 1), new Range(3, 3)}));
  }

  @ParameterizedTest
  @MethodSource
  void should_merge(RangeSet rs, RangeSet other, Range... expected) {
    rs.merge(other);
    assertThat(rs.stream()).containsExactly(expected);
  }

  static Stream<Arguments> should_merge() {
    // Note: more test cases are covered in RangeUtilsTest.
    return Stream.of(
        Arguments.of(RangeSet.of(), RangeSet.of(), new Range[] {}),
        Arguments.of(
            RangeSet.of(new Range(0, 0)),
            RangeSet.of(new Range(0, 0)),
            new Range[] {new Range(0, 0)}),
        Arguments.of(
            RangeSet.of(new Range(0, 2)),
            RangeSet.of(new Range(1, 3)),
            new Range[] {new Range(0, 3)}),
        Arguments.of(
            RangeSet.of(new Range(1, 2)),
            RangeSet.of(new Range(3, 3)),
            new Range[] {new Range(1, 3)}),
        Arguments.of(
            RangeSet.of(new Range(2, 3)),
            RangeSet.of(new Range(1, 1)),
            new Range[] {new Range(1, 3)}),
        Arguments.of(
            RangeSet.of(new Range(0, 3)),
            RangeSet.of(new Range(1, 2)),
            new Range[] {new Range(0, 3)}),
        Arguments.of(
            RangeSet.of(new Range(1, 2)),
            RangeSet.of(new Range(0, 3)),
            new Range[] {new Range(0, 3)}),
        Arguments.of(
            RangeSet.of(new Range(1, 2)),
            RangeSet.of(new Range(3, 4)),
            new Range[] {new Range(1, 4)}),
        Arguments.of(
            RangeSet.of(new Range(3, 4)),
            RangeSet.of(new Range(1, 2)),
            new Range[] {new Range(1, 4)}),
        Arguments.of(
            RangeSet.of(new Range(1, 2)),
            RangeSet.of(new Range(4, 5)),
            new Range[] {new Range(1, 2), new Range(4, 5)}),
        Arguments.of(
            RangeSet.of(new Range(4, 5)),
            RangeSet.of(new Range(1, 2)),
            new Range[] {new Range(1, 2), new Range(4, 5)}),
        Arguments.of(
            RangeSet.of(new Range(3, 4)),
            RangeSet.of(new Range(1, 2), new Range(5, 6)),
            new Range[] {new Range(1, 6)}),
        Arguments.of(
            RangeSet.of(new Range(1, 2), new Range(5, 6)),
            RangeSet.of(new Range(3, 4), new Range(6, 7)),
            new Range[] {new Range(1, 7)}));
  }

  @Test
  void should_report_empty() {
    assertThat(RangeSet.of().isEmpty()).isTrue();
    assertThat(RangeSet.of(new Range(0)).isEmpty()).isFalse();
  }

  @Test
  void should_report_size() {
    assertThat(RangeSet.of().size()).isEqualTo(0);
    assertThat(RangeSet.of(new Range(0)).size()).isEqualTo(1);
    assertThat(RangeSet.of(new Range(0), new Range(1)).size()).isEqualTo(1);
    assertThat(RangeSet.of(new Range(0), new Range(1), new Range(2)).size()).isEqualTo(1);
    assertThat(RangeSet.of(new Range(0), new Range(2)).size()).isEqualTo(2);
    assertThat(RangeSet.of(new Range(0), new Range(2), new Range(4)).size()).isEqualTo(3);
  }

  @Test
  void should_report_sum() {
    assertThat(RangeSet.of().sum()).isEqualTo(0);
    assertThat(RangeSet.of(new Range(0)).sum()).isEqualTo(1);
    assertThat(RangeSet.of(new Range(0), new Range(1)).sum()).isEqualTo(2);
    assertThat(RangeSet.of(new Range(1), new Range(2, 5), new Range(6, 10)).sum()).isEqualTo(10);
  }
}
