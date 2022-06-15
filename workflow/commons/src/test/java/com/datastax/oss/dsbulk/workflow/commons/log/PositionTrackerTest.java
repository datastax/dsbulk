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
package com.datastax.oss.dsbulk.workflow.commons.log;

import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PositionTrackerTest {

  private static final URI RESOURCE1 = URI.create("file://data1.csv");
  private static final URI RESOURCE2 = URI.create("file://data2.csv");

  @Test
  void should_update_position() {
    PositionTracker tracker = new PositionTracker();
    tracker.update(RESOURCE1, 3);
    assertThat(tracker.getPositions().get(RESOURCE1)).containsExactly(new Range(3L));
    tracker.update(RESOURCE1, 1);
    assertThat(tracker.getPositions().get(RESOURCE1)).containsExactly(new Range(1L), new Range(3L));
    tracker.update(RESOURCE1, 2);
    assertThat(tracker.getPositions().get(RESOURCE1)).containsExactly(new Range(1L, 3L));
    tracker.update(RESOURCE1, 2);
    assertThat(tracker.getPositions().get(RESOURCE1)).containsExactly(new Range(1L, 3L));
    tracker.update(RESOURCE1, 6);
    assertThat(tracker.getPositions().get(RESOURCE1))
        .containsExactly(new Range(1L, 3L), new Range(6L));
    tracker.update(RESOURCE1, 5);
    assertThat(tracker.getPositions().get(RESOURCE1))
        .containsExactly(new Range(1L, 3L), new Range(5L, 6L));
    tracker.update(RESOURCE1, 4);
    assertThat(tracker.getPositions().get(RESOURCE1)).containsExactly(new Range(1L, 6L));
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_update_positions() {
    return Lists.newArrayList(
        arguments(new long[] {1, 2, 3, 4}, ranges(new Range(1L, 4L))),
        arguments(new long[] {1, 2, 3, 5}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {5, 3, 2, 1}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {1, 3, 5, 4, 2}, ranges(new Range(1L, 5L))),
        arguments(new long[] {1, 2, 4, 5, 3}, ranges(new Range(1L, 5L))),
        arguments(new long[] {2, 4, 5, 3, 1}, ranges(new Range(1L, 5L))),
        arguments(new long[] {4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {3, 2}, ranges(new Range(2L, 3L))),
        arguments(new long[] {3, 5, 4, 2}, ranges(new Range(2L, 5L))));
  }

  @ParameterizedTest
  @MethodSource
  final void should_update_positions(long[] positions, List<Range> expected) {
    PositionTracker positionTracker = new PositionTracker();
    for (long position : positions) {
      positionTracker.update(RESOURCE1, position);
    }
    assertThat(positionTracker.getPositions()).hasSize(1).containsEntry(RESOURCE1, expected);
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

        // contiguous after
        arguments(ranges(r(0, 2)), r(3, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2)), r(0, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2)), r(2, 3), ranges(r(0, 3))),
        arguments(ranges(r(0, 2), r(5, 8)), r(3, 3), ranges(r(0, 3), r(5, 8))),
        arguments(ranges(r(0, 2), r(5, 8)), r(0, 3), ranges(r(0, 3), r(5, 8))),
        arguments(ranges(r(0, 2), r(5, 8)), r(1, 3), ranges(r(0, 3), r(5, 8))),

        // merge with next
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 3), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(1, 3), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 4), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 5), ranges(r(0, 5))),
        arguments(ranges(r(0, 2), r(4, 5)), r(3, 6), ranges(r(0, 6))),
        arguments(ranges(r(0, 2), r(6, 8)), r(3, 5), ranges(r(0, 8))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 7), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 8), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 10), ranges(r(0, 10))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(3, 11), ranges(r(0, 11))),
        arguments(ranges(r(0, 2), r(4, 6), r(8, 10)), r(0, 100), ranges(r(0, 100))));
  }

  @ParameterizedTest
  @MethodSource
  final void should_add_ranges(List<Range> actual, Range range, List<Range> expected) {
    PositionTracker.addRange(actual, range);
    assertThat(actual).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource
  void should_merge(PositionTracker actual, PositionTracker child, PositionTracker expected) {
    actual.merge(child);
    assertThat(actual.getPositions()).isEqualTo(expected.getPositions());
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_merge() {
    return Lists.newArrayList(
        arguments(new PositionTracker(), new PositionTracker(), new PositionTracker()),
        arguments(
            tracker(RESOURCE1, ranges(r(0, 0))),
            new PositionTracker(),
            tracker(RESOURCE1, ranges(r(0, 0)))),
        arguments(
            new PositionTracker(),
            tracker(RESOURCE1, ranges(r(0, 0))),
            tracker(RESOURCE1, ranges(r(0, 0)))),
        arguments(
            tracker(RESOURCE1, ranges(r(0, 2))),
            tracker(RESOURCE1, ranges(r(3, 5))),
            tracker(RESOURCE1, ranges(r(0, 5)))),
        arguments(
            tracker(RESOURCE1, ranges(r(0, 2), r(4, 5))),
            tracker(RESOURCE1, ranges(r(3, 3))),
            tracker(RESOURCE1, ranges(r(0, 5)))),
        arguments(
            tracker(RESOURCE1, ranges(r(0, 0))),
            tracker(RESOURCE2, ranges(r(0, 0))),
            tracker(ranges(r(0, 0)), ranges(r(0, 0)))),
        arguments(
            tracker(ranges(r(0, 2), r(4, 5)), ranges(r(0, 2), r(4, 5))),
            tracker(RESOURCE1, ranges(r(3, 3))),
            tracker(ranges(r(0, 5)), ranges(r(0, 2), r(4, 5)))));
  }

  private static PositionTracker tracker(URI resource, List<Range> ranges) {
    PositionTracker tracker = new PositionTracker();
    tracker.getPositions().put(resource, ranges);
    return tracker;
  }

  private static PositionTracker tracker(List<Range> ranges1, List<Range> ranges2) {
    PositionTracker tracker = new PositionTracker();
    tracker.getPositions().put(PositionTrackerTest.RESOURCE1, ranges1);
    tracker.getPositions().put(PositionTrackerTest.RESOURCE2, ranges2);
    return tracker;
  }

  static List<Range> ranges(Range... ranges) {
    return ranges == null ? newArrayList() : newArrayList(ranges);
  }

  private static Range r(int lower, int upper) {
    return new Range(lower, upper);
  }
}
