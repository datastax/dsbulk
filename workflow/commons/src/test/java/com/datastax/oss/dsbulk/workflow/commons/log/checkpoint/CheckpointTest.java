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

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CheckpointTest {

  @ParameterizedTest
  @MethodSource
  void should_parse(String text, Checkpoint expected) {
    Checkpoint actual = Checkpoint.parse(text);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> should_parse() {
    return Stream.of(
        Arguments.of(
            "0;10;1:10;", new Checkpoint(10, RangeSet.of(new Range(1, 10)), RangeSet.of(), false)),
        Arguments.of(
            "1;10;1:10;", new Checkpoint(10, RangeSet.of(new Range(1, 10)), RangeSet.of(), true)),
        Arguments.of(
            "0;30;1:10,12:20,30:30;",
            new Checkpoint(
                30,
                RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30)),
                RangeSet.of(),
                false)),
        Arguments.of(
            "0;10;;1:10", new Checkpoint(10, RangeSet.of(), RangeSet.of(new Range(1, 10)), false)),
        Arguments.of(
            "1;10;;1:10", new Checkpoint(10, RangeSet.of(), RangeSet.of(new Range(1, 10)), true)),
        Arguments.of(
            "0;30;;1:10,12:20,30:30",
            new Checkpoint(
                30,
                RangeSet.of(),
                RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30)),
                false)),
        Arguments.of("0;0;;", new Checkpoint(0, RangeSet.of(), RangeSet.of(), false)),
        Arguments.of("1;10;;", new Checkpoint(10, RangeSet.of(), RangeSet.of(), true)));
  }

  @ParameterizedTest
  @MethodSource
  void should_serialize_as_text(Checkpoint cp, String expected) {
    assertThat(cp.asCsv()).isEqualTo(expected);
  }

  static Stream<Arguments> should_serialize_as_text() {
    return Stream.of(
        Arguments.of(new Checkpoint(0, RangeSet.of(), RangeSet.of(), false), "0;0;;"),
        Arguments.of(new Checkpoint(0, RangeSet.of(), RangeSet.of(), true), "1;0;;"),
        Arguments.of(
            new Checkpoint(
                30,
                RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30)),
                RangeSet.of(),
                false),
            "0;30;1:10,12:20,30;"),
        Arguments.of(
            new Checkpoint(10, RangeSet.of(new Range(1, 10)), RangeSet.of(), true), "1;10;1:10;"),
        Arguments.of(
            new Checkpoint(
                30,
                RangeSet.of(),
                RangeSet.of(new Range(1, 10), new Range(12, 20), new Range(30)),
                false),
            "0;30;;1:10,12:20,30"),
        Arguments.of(
            new Checkpoint(10, RangeSet.of(), RangeSet.of(new Range(1, 10)), true), "1;10;;1:10"));
  }

  @ParameterizedTest
  @MethodSource
  void should_merge(Checkpoint cp, Checkpoint other, Checkpoint expected) {
    cp.merge(other);
    assertThat(cp).isEqualTo(expected);
  }

  static Stream<Arguments> should_merge() {
    return Stream.of(
        Arguments.of(new Checkpoint(), new Checkpoint(), new Checkpoint()),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(2, RangeSet.of(r(3, 4)), RangeSet.of(), false),
            new Checkpoint(4, RangeSet.of(r(1, 4)), RangeSet.of(), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(3, 4)), RangeSet.of(), false),
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(4, RangeSet.of(r(1, 4)), RangeSet.of(), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(2, RangeSet.of(r(4, 5)), RangeSet.of(), false),
            new Checkpoint(4, RangeSet.of(r(1, 2), r(4, 5)), RangeSet.of(), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(4, 5)), RangeSet.of(), false),
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(4, RangeSet.of(r(1, 2), r(4, 5)), RangeSet.of(), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), true),
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(4, RangeSet.of(r(1, 2)), RangeSet.of(), true)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), false),
            new Checkpoint(2, RangeSet.of(r(1, 2)), RangeSet.of(), true),
            new Checkpoint(4, RangeSet.of(r(1, 2)), RangeSet.of(), true)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), false),
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(3, 4)), false),
            new Checkpoint(4, RangeSet.of(), RangeSet.of(r(1, 4)), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), false),
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(4, 5)), false),
            new Checkpoint(4, RangeSet.of(), RangeSet.of(r(1, 2), r(4, 5)), false)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), true),
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), false),
            new Checkpoint(4, RangeSet.of(), RangeSet.of(r(1, 2)), true)),
        Arguments.of(
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), false),
            new Checkpoint(2, RangeSet.of(), RangeSet.of(r(1, 2)), true),
            new Checkpoint(4, RangeSet.of(), RangeSet.of(r(1, 2)), true)));
  }

  @Test
  void should_increment_produced() {
    Checkpoint cp = new Checkpoint();
    cp.incrementProduced();
    assertThat(cp.getProduced()).isEqualTo(1);
    cp.incrementProduced();
    assertThat(cp.getProduced()).isEqualTo(2);
    cp.incrementProduced();
    assertThat(cp.getProduced()).isEqualTo(3);
  }

  @Test
  void should_update_consumed() {
    Checkpoint cp = new Checkpoint();
    cp.updateConsumed(1, true);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 1));
    assertThat(cp.getConsumedFailed().stream()).isEmpty();
    cp.updateConsumed(2, true);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 2));
    assertThat(cp.getConsumedFailed().stream()).isEmpty();
    cp.updateConsumed(5, true);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 2), r(5, 5));
    assertThat(cp.getConsumedFailed().stream()).isEmpty();
    cp.updateConsumed(4, true);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 2), r(4, 5));
    assertThat(cp.getConsumedFailed().stream()).isEmpty();
    cp.updateConsumed(3, true);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).isEmpty();
    cp.updateConsumed(1, false);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).containsExactly(r(1, 1));
    cp.updateConsumed(2, false);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).containsExactly(r(1, 2));
    cp.updateConsumed(5, false);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).containsExactly(r(1, 2), r(5, 5));
    cp.updateConsumed(4, false);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).containsExactly(r(1, 2), r(4, 5));
    cp.updateConsumed(3, false);
    assertThat(cp.getConsumedSuccessful().stream()).containsExactly(r(1, 5));
    assertThat(cp.getConsumedFailed().stream()).containsExactly(r(1, 5));
  }
}
