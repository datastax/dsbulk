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

import static com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.RangeUtilsTest.r;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CheckpointManagerTest {

  private static final URI RESOURCE1 = URI.create("file://data1.csv");
  private static final URI RESOURCE2 = URI.create("file://data2.csv");

  @Test
  void should_update_manager_with_successful_positions() {
    CheckpointManager manager = new CheckpointManager();
    manager.update(RESOURCE1, 3, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(3L));
    manager.update(RESOURCE1, 1, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L), new Range(3L));
    manager.update(RESOURCE1, 2, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L, 3L));
    manager.update(RESOURCE1, 2, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L, 3L));
    manager.update(RESOURCE1, 6, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L, 3L), new Range(6L));
    manager.update(RESOURCE1, 5, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L, 3L), new Range(5L, 6L));
    manager.update(RESOURCE1, 4, true);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1L, 6L));
  }

  @Test
  void should_update_manager_with_failed_positions() {
    CheckpointManager manager = new CheckpointManager();
    manager.update(RESOURCE1, 3, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(3L));
    manager.update(RESOURCE1, 1, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L), new Range(3L));
    manager.update(RESOURCE1, 2, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L, 3L));
    manager.update(RESOURCE1, 2, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L, 3L));
    manager.update(RESOURCE1, 6, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L, 3L), new Range(6L));
    manager.update(RESOURCE1, 5, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L, 3L), new Range(5L, 6L));
    manager.update(RESOURCE1, 4, false);
    assertThat(manager.checkpoints.get(RESOURCE1).getConsumedFailed().stream())
        .containsExactly(new Range(1L, 6L));
  }

  @ParameterizedTest
  @MethodSource
  void should_merge_managers(
      CheckpointManager actual, CheckpointManager child, CheckpointManager expected) {
    actual.merge(child);
    assertThat(actual).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_merge_managers() {
    return Lists.newArrayList(
        arguments(new CheckpointManager(), new CheckpointManager(), new CheckpointManager()),
        arguments(
            manager(0, RangeSet.of(r(0, 0)), RangeSet.of()),
            new CheckpointManager(),
            manager(0, RangeSet.of(r(0, 0)), RangeSet.of())),
        arguments(
            new CheckpointManager(),
            manager(0, RangeSet.of(r(0, 0)), RangeSet.of()),
            manager(0, RangeSet.of(r(0, 0)), RangeSet.of())),
        arguments(
            manager(3, RangeSet.of(r(0, 2)), RangeSet.of()),
            manager(2, RangeSet.of(r(3, 5)), RangeSet.of()),
            manager(5, RangeSet.of(r(0, 5)), RangeSet.of())),
        arguments(
            manager(5, RangeSet.of(r(0, 2), r(4, 5)), RangeSet.of()),
            manager(1, RangeSet.of(r(3, 3)), RangeSet.of()),
            manager(6, RangeSet.of(r(0, 5)), RangeSet.of())),
        arguments(
            manager(
                5,
                RangeSet.of(r(1, 2), r(4, 5)),
                RangeSet.of(r(3, 3)),
                5,
                RangeSet.of(r(6, 9)),
                RangeSet.of(r(10, 10))),
            manager(
                5,
                RangeSet.of(r(11, 12), r(14, 15)),
                RangeSet.of(r(13, 13)),
                5,
                RangeSet.of(r(16, 19)),
                RangeSet.of(r(20, 20))),
            manager(
                10,
                RangeSet.of(r(1, 2), r(4, 5), r(11, 12), r(14, 15)),
                RangeSet.of(r(3, 3), r(13, 13)),
                10,
                RangeSet.of(r(6, 9), r(16, 19)),
                RangeSet.of(r(10, 10), r(20, 20)))));
  }

  @ParameterizedTest
  @MethodSource
  void should_parse(String text, Map<URI, Checkpoint> expected) throws IOException {
    CheckpointManager manager = CheckpointManager.parse(new BufferedReader(new StringReader(text)));
    assertThat(manager.checkpoints).isEqualTo(expected);
  }

  static Stream<Arguments> should_parse() {
    return Stream.of(
        Arguments.of(
            "file://file.csv;1;20;1:10;11:20",
            ImmutableMap.of(
                URI.create("file://file.csv"),
                new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), true))),
        Arguments.of(
            "file://file1.csv;1;20;1:10;11:20\nfile://file2.csv;0;0;;",
            ImmutableMap.of(
                URI.create("file://file1.csv"),
                new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), true),
                URI.create("file://file2.csv"),
                new Checkpoint(0, RangeSet.of(), RangeSet.of(), false))));
  }

  @ParameterizedTest
  @MethodSource
  void should_print(CheckpointManager manager, String expected) {
    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    manager.printCsv(writer);
    assertThat(sw.toString()).isEqualTo(expected);
  }

  static Stream<Arguments> should_print() {
    return Stream.of(
        Arguments.of(
            new CheckpointManager(ImmutableMap.of(RESOURCE1, new Checkpoint())),
            "file://data1.csv;0;0;;" + System.lineSeparator()),
        Arguments.of(
            new CheckpointManager(
                ImmutableMap.of(
                    RESOURCE1,
                    new Checkpoint(10, RangeSet.of(r(1, 5)), RangeSet.of(r(6, 10)), true))),
            "file://data1.csv;1;10;1:5;6:10" + System.lineSeparator()),
        Arguments.of(
            new CheckpointManager(
                ImmutableMap.of(
                    RESOURCE1, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), false))),
            "file://data1.csv;0;10;1:10;" + System.lineSeparator()),
        Arguments.of(
            new CheckpointManager(
                ImmutableMap.of(
                    RESOURCE1,
                    new Checkpoint(
                        30, RangeSet.of(r(1, 10), r(20, 30)), RangeSet.of(r(11, 19)), false))),
            "file://data1.csv;0;30;1:10,20:30;11:19" + System.lineSeparator()));
  }

  private static CheckpointManager manager(long produced, RangeSet consumed, RangeSet failed) {
    CheckpointManager manager = new CheckpointManager();
    manager.checkpoints.put(
        CheckpointManagerTest.RESOURCE1, new Checkpoint(produced, consumed, failed, false));
    return manager;
  }

  private static CheckpointManager manager(
      long produced1,
      RangeSet successful1,
      RangeSet failed1,
      long produced2,
      RangeSet successful2,
      RangeSet failed2) {
    CheckpointManager manager = new CheckpointManager();
    manager.checkpoints.put(RESOURCE1, new Checkpoint(produced1, successful1, failed1, false));
    manager.checkpoints.put(RESOURCE2, new Checkpoint(produced2, successful2, failed2, false));
    return manager;
  }
}
