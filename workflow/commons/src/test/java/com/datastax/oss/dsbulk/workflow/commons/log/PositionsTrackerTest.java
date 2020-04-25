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
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URI;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PositionsTrackerTest {

  private static final URI RESOURCE = URI.create("file://data.csv");

  @Test
  void should_update_position() {
    PositionsTracker tracker = new PositionsTracker();
    tracker.update(RESOURCE, 3);
    assertThat(tracker.getPositions().get(RESOURCE)).containsExactly(new Range(3L));
    tracker.update(RESOURCE, 1);
    assertThat(tracker.getPositions().get(RESOURCE)).containsExactly(new Range(1L), new Range(3L));
    tracker.update(RESOURCE, 2);
    assertThat(tracker.getPositions().get(RESOURCE)).containsExactly(new Range(1L, 3L));
    tracker.update(RESOURCE, 2);
    assertThat(tracker.getPositions().get(RESOURCE)).containsExactly(new Range(1L, 3L));
    tracker.update(RESOURCE, 6);
    assertThat(tracker.getPositions().get(RESOURCE))
        .containsExactly(new Range(1L, 3L), new Range(6L));
    tracker.update(RESOURCE, 5);
    assertThat(tracker.getPositions().get(RESOURCE))
        .containsExactly(new Range(1L, 3L), new Range(5L, 6L));
    tracker.update(RESOURCE, 4);
    assertThat(tracker.getPositions().get(RESOURCE)).containsExactly(new Range(1L, 6L));
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_update_positions() {
    return Lists.newArrayList(
        arguments(new long[] {1, 2, 3, 4}, ranges(new Range(1L, 4L))),
        arguments(new long[] {1, 2, 3, 5}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {5, 3, 2, 1}, ranges(new Range(1L, 3L), new Range(5L))),
        arguments(new long[] {1, 3, 5, 4, 2}, ranges(new Range(1L, 5L))),
        arguments(new long[] {2, 4, 5, 3, 1}, ranges(new Range(1L, 5L))),
        arguments(new long[] {4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {4, 3, 2, 1}, ranges(new Range(1L, 4L))),
        arguments(new long[] {3, 2}, ranges(new Range(2L, 3L))),
        arguments(new long[] {3, 5, 4, 2}, ranges(new Range(2L, 5L))));
  }

  @ParameterizedTest
  @MethodSource
  final void should_update_positions(long[] positions, List<Range> expected) {
    PositionsTracker positionsTracker = new PositionsTracker();
    for (long position : positions) {
      positionsTracker.update(RESOURCE, position);
    }
    assertThat(positionsTracker.getPositions()).hasSize(1).containsEntry(RESOURCE, expected);
  }

  static List<Range> ranges(Range... ranges) {
    return ranges == null ? emptyList() : newArrayList(ranges);
  }
}
