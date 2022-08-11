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
import static com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy.resume;
import static com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy.retry;
import static com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy.rewind;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ReplayStrategyTest {

  @ParameterizedTest
  @MethodSource
  void isComplete(ReplayStrategy strategy, Checkpoint cp, boolean expected) {
    assertThat(strategy.isComplete(cp)).isEqualTo(expected);
  }

  static Stream<Arguments> isComplete() {
    return Stream.of(
        Arguments.of(resume, new Checkpoint(), false),
        Arguments.of(retry, new Checkpoint(), false),
        Arguments.of(rewind, new Checkpoint(), false),
        Arguments.of(resume, new Checkpoint(0, RangeSet.of(), RangeSet.of(), false), false),
        Arguments.of(retry, new Checkpoint(0, RangeSet.of(), RangeSet.of(), false), false),
        Arguments.of(rewind, new Checkpoint(0, RangeSet.of(), RangeSet.of(), false), false),
        Arguments.of(
            resume, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), false), false),
        Arguments.of(retry, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), false), false),
        Arguments.of(
            rewind, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), false), false),
        Arguments.of(resume, new Checkpoint(0, RangeSet.of(), RangeSet.of(), true), true),
        Arguments.of(retry, new Checkpoint(0, RangeSet.of(), RangeSet.of(), true), true),
        Arguments.of(rewind, new Checkpoint(0, RangeSet.of(), RangeSet.of(), true), true),
        Arguments.of(resume, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), true), true),
        Arguments.of(retry, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), true), true),
        Arguments.of(rewind, new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), true), true),
        Arguments.of(
            resume, new Checkpoint(10, RangeSet.of(r(1, 5)), RangeSet.of(r(6, 10)), true), true),
        Arguments.of(
            retry, new Checkpoint(10, RangeSet.of(r(1, 5)), RangeSet.of(r(6, 10)), true), false),
        Arguments.of(
            rewind, new Checkpoint(10, RangeSet.of(r(1, 5)), RangeSet.of(r(6, 10)), true), false));
  }

  @ParameterizedTest
  @MethodSource
  void reset(ReplayStrategy strategy, Checkpoint cp, Checkpoint expected) {
    strategy.reset(cp);
    assertThat(cp).isEqualTo(expected);
  }

  static Stream<Arguments> reset() {
    return Stream.of(
        Arguments.of(resume, new Checkpoint(), new Checkpoint()),
        Arguments.of(retry, new Checkpoint(), new Checkpoint()),
        Arguments.of(rewind, new Checkpoint(), new Checkpoint()),
        Arguments.of(
            resume,
            new Checkpoint(21, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), true),
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false)),
        Arguments.of(
            retry,
            new Checkpoint(21, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), true),
            new Checkpoint(10, RangeSet.of(r(1, 10)), RangeSet.of(), false)),
        Arguments.of(
            rewind,
            new Checkpoint(21, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), true),
            new Checkpoint(0, RangeSet.of(), RangeSet.of(), false)));
  }

  @ParameterizedTest
  @MethodSource
  void shouldReplay(ReplayStrategy strategy, Checkpoint cp, long position, boolean expected) {
    assertThat(strategy.shouldReplay(cp, position)).isEqualTo(expected);
  }

  static Stream<Arguments> shouldReplay() {
    return Stream.of(
        Arguments.of(resume, new Checkpoint(), 1, true),
        Arguments.of(retry, new Checkpoint(), 1, true),
        Arguments.of(rewind, new Checkpoint(), 1, true),
        Arguments.of(
            resume,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            1,
            false),
        Arguments.of(
            retry,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            1,
            false),
        Arguments.of(
            rewind,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            1,
            true),
        Arguments.of(
            resume,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            11,
            false),
        Arguments.of(
            retry,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            11,
            true),
        Arguments.of(
            rewind,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            11,
            true));
  }

  @ParameterizedTest
  @MethodSource
  void getTotalItems(ReplayStrategy strategy, Checkpoint cp, long expected) {
    assertThat(strategy.getTotalItems(cp)).isEqualTo(expected);
  }

  static Stream<Arguments> getTotalItems() {
    return Stream.of(
        Arguments.of(resume, new Checkpoint(), 0),
        Arguments.of(retry, new Checkpoint(), 0),
        Arguments.of(rewind, new Checkpoint(), 0),
        Arguments.of(
            resume, new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false), 20),
        Arguments.of(
            retry,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            10,
            false),
        Arguments.of(
            rewind,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            0,
            true));
  }

  @ParameterizedTest
  @MethodSource
  void getRejectedItems(ReplayStrategy strategy, Checkpoint cp, long expected) {
    assertThat(strategy.getRejectedItems(cp)).isEqualTo(expected);
  }

  static Stream<Arguments> getRejectedItems() {
    return Stream.of(
        Arguments.of(resume, new Checkpoint(), 0),
        Arguments.of(retry, new Checkpoint(), 0),
        Arguments.of(rewind, new Checkpoint(), 0),
        Arguments.of(
            resume, new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false), 10),
        Arguments.of(
            retry,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            0,
            false),
        Arguments.of(
            rewind,
            new Checkpoint(20, RangeSet.of(r(1, 10)), RangeSet.of(r(11, 20)), false),
            0,
            true));
  }
}
