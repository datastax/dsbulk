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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.util.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RangeUtilsTest {

  @ParameterizedTest
  @MethodSource
  final void should_update_positions(long[] positions, List<Range> expected) {
    List<Range> actual = new ArrayList<>();
    for (long position : positions) {
      RangeUtils.addPosition(actual, position);
    }
    assertThat(actual).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_update_positions() {
    return Lists.newArrayList(
        arguments(new long[] {1, 2, 3, 4}, ranges(new Range(1L, 4L))),
        arguments(new long[] {1, 1, 2, 2, 3, 3, 4, 4}, ranges(new Range(1L, 4L))),
        arguments(new long[] {1, 2, 3, 5}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {1, 2, 3, 5, 1, 2, 3, 5}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {4, 3, 2, 1, 4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {5, 3, 2, 1}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {1, 3, 5, 4, 2}, ranges(new Range(1L, 5L))),
        arguments(new long[] {1, 2, 4, 5, 3}, ranges(new Range(1L, 5L))),
        arguments(new long[] {1, 2, 5, 6, 3}, ranges(new Range(1L, 3L), new Range(5L, 6L))),
        arguments(new long[] {1, 2, 4, 5, 8, 3}, ranges(new Range(1L, 5L), new Range(8L))),
        arguments(
            new long[] {1, 2, 5, 6, 8, 4},
            ranges(new Range(1L, 2L), new Range(4L, 6L), new Range(8L))),
        arguments(
            new long[] {1, 2, 5, 6, 8, 3},
            ranges(new Range(1L, 3L), new Range(5L, 6L), new Range(8L))),
        arguments(new long[] {2, 4, 5, 3, 1}, ranges(new Range(1L, 5L))),
        arguments(new long[] {3, 2}, ranges(new Range(2L, 3L))),
        arguments(new long[] {3, 5, 4, 2}, ranges(new Range(2L, 5L))));
  }

  @ParameterizedTest
  @MethodSource
  final void should_add_ranges(List<Range> actual, Range range, List<Range> expected) {
    RangeUtils.addRange(actual, range);
    assertThat(actual).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_add_ranges() {
    return Lists.newArrayList(
        arguments(ranges(), r(1, 3), ranges(r(1, 3))),

        // disjoint insertion

        arguments(ranges(r(1, 3)), r(5, 8), ranges(r(1, 3), r(5, 8))),
        arguments(ranges(r(5, 8)), r(1, 3), ranges(r(1, 3), r(5, 8))),
        arguments(ranges(r(1, 3), r(10, 13)), r(5, 8), ranges(r(1, 3), r(5, 8), r(10, 13))),

        // contiguous before
        arguments(ranges(r(5, 8)), r(2, 4), ranges(r(2, 8))),
        arguments(ranges(r(5, 8)), r(4, 4), ranges(r(4, 8))),
        arguments(ranges(r(0, 0), r(5, 8)), r(2, 4), ranges(r(0, 0), r(2, 8))),
        arguments(ranges(r(0, 0), r(5, 8)), r(2, 5), ranges(r(0, 0), r(2, 8))),
        arguments(
            ranges(r(0, 0), r(5, 8), r(10, 13)), r(2, 8), ranges(r(0, 0), r(2, 8), r(10, 13))),
        arguments(
            ranges(r(0, 2), r(5, 8), r(10, 13)), r(4, 8), ranges(r(0, 2), r(4, 8), r(10, 13))),

        // contained
        arguments(
            ranges(r(1, 3), r(5, 8), r(10, 13)), r(5, 8), ranges(r(1, 3), r(5, 8), r(10, 13))),
        arguments(
            ranges(r(1, 3), r(5, 8), r(10, 13)), r(6, 7), ranges(r(1, 3), r(5, 8), r(10, 13))),

        // spanning
        arguments(
            ranges(r(0, 2), r(5, 8), r(11, 13)), r(4, 9), ranges(r(0, 2), r(4, 9), r(11, 13))),
        arguments(
            ranges(r(0, 1), r(5, 8), r(12, 13)), r(3, 10), ranges(r(0, 1), r(3, 10), r(12, 13))),

        // contiguous
        arguments(ranges(r(0, 2)), r(3, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2)), r(0, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2)), r(2, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2), r(5, 8)), r(3, 3), ranges(r(0, 3), r(5, 8))),
        arguments(ranges(r(0, 2), r(5, 8)), r(0, 3), ranges(r(0, 3), r(5, 8))),
        arguments(ranges(r(0, 2), r(5, 8)), r(1, 3), ranges(r(0, 3), r(5, 8))),
        arguments(ranges(r(1, 3)), r(0, 0), ranges(r(0, 3))),
        arguments(ranges(r(1, 3)), r(0, 3), ranges(r(0, 3))),
        arguments(ranges(r(1, 3)), r(0, 1), ranges(r(0, 3))),

        // merges
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 3), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(1, 3), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 4), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 5), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 6), ranges(r(0, 6))),
        arguments(ranges(r(0, 2), r(6, 8)), r(3, 5), ranges(r(0, 8))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 7), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 8), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 9), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 10), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 11), ranges(r(0, 11))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(2, 7), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(1, 7), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(0, 7), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(-1, 7), ranges(r(-1, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(0, 100), ranges(r(0, 100))));
  }

  @ParameterizedTest
  @MethodSource
  void should_contain_position(List<Range> ranges, long position, boolean expected) {
    assertThat(RangeUtils.contains(ranges, position)).isEqualTo(expected);
  }

  static Stream<Arguments> should_contain_position() {
    return Stream.of(
        Arguments.of(ranges(), 0, false),
        Arguments.of(ranges(new Range(0, 0)), 0, true),
        Arguments.of(ranges(new Range(1, 1)), 0, false),
        Arguments.of(ranges(new Range(0, 0)), 1, false),
        Arguments.of(ranges(new Range(0, 0), new Range(2, 2)), 1, false),
        Arguments.of(ranges(new Range(0, 0), new Range(2, 2)), 2, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 2, false),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 5, false),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 0, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 1, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 3, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 4, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 6, true),
        Arguments.of(ranges(new Range(0, 1), new Range(3, 4), new Range(6, 7)), 7, true));
  }

  private static List<Range> ranges(Range... ranges) {
    return new ArrayList<>(Arrays.asList(ranges));
  }

  static Range r(int lower, int upper) {
    return new Range(lower, upper);
  }
}
