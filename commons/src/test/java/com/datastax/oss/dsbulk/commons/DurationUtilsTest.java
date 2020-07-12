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
package com.datastax.oss.dsbulk.commons;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DurationUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "PT23H                   , DAYS        , P0D",
    "PT59M                   , HOURS       , PT0H",
    "PT59S                   , MINUTES     , PT0M",
    "PT0.999S                , SECONDS     , PT0S",
    "P1DT23H                 , DAYS        , P1D",
    "P1DT23H59M              , HOURS       , P1DT23H",
    "P10DT12H34M56S          , MINUTES     , P10DT12H34M",
    "P10DT12H34M56.789S      , SECONDS     , P10DT12H34M56S",
    "P10DT12H34M56.789123456S, MILLISECONDS, P10DT12H34M56.789S",
    "P10DT12H34M56.789123456S, NANOSECONDS , P10DT12H34M56.789123456S",
  })
  void should_round_duration(String input, TimeUnit unit, String expected) {
    // given
    Duration duration = Duration.parse(input);
    // when
    Duration rounded = DurationUtils.round(duration, unit);
    // then
    assertThat(rounded).isEqualTo(Duration.parse(expected));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "P1000D                   | 1,000 days",
        "P10D                     | 10 days",
        "P1D                      | 1 day",
        "P1DT24H                  | 2 days",
        "P1DT23H                  | 1 day and 23 hours",
        "P1DT1H                   | 1 day and 1 hour",
        "P1DT1H1M                 | 1 day, 1 hour and 1 minute",
        "P1DT23H59M               | 1 day, 23 hours and 59 minutes",
        "P1DT1H1M1S               | 1 day, 1 hour, 1 minute and 1 second",
        "P10DT12H34M56S           | 10 days, 12 hours, 34 minutes and 56 seconds",
        "P1DT1H1M1.001S           | 1 day, 1 hour, 1 minute, 1 second and 1 millisecond",
        "P10DT12H34M56.789S       | 10 days, 12 hours, 34 minutes, 56 seconds and 789 milliseconds",
        "P1DT1H1M1.001000001S     | 1 day, 1 hour, 1 minute, 1 second, 1 millisecond and 1 nanosecond",
        "P10DT12H34M56.789123456S | 10 days, 12 hours, 34 minutes, 56 seconds, 789 milliseconds and 123,456 nanoseconds",
        "PT12H34M56.789123456S    | 12 hours, 34 minutes, 56 seconds, 789 milliseconds and 123,456 nanoseconds",
        "PT34M56.789123456S       | 34 minutes, 56 seconds, 789 milliseconds and 123,456 nanoseconds",
        "PT56.789123456S          | 56 seconds, 789 milliseconds and 123,456 nanoseconds",
        "PT0.789123456S           | 789 milliseconds and 123,456 nanoseconds",
        "PT0.000123456S           | 123,456 nanoseconds",
        "PT1H                     | 1 hour",
        "PT1M                     | 1 minute",
        "PT1S                     | 1 second",
        "PT0.1S                   | 100 milliseconds",
        "PT0.01S                  | 10 milliseconds",
        "PT0.001S                 | 1 millisecond",
        "PT0.0001S                | 100,000 nanoseconds",
        "PT0.000000001S           | 1 nanosecond",
      })
  void should_format_duration(String input, String expected) {
    // given
    Duration duration = Duration.parse(input);
    // when
    String formatted = DurationUtils.formatDuration(duration);
    // then
    assertThat(formatted).isEqualTo(expected);
  }
}
