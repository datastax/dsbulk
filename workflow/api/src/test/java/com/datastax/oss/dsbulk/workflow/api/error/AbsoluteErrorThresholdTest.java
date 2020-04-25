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
package com.datastax.oss.dsbulk.workflow.api.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AbsoluteErrorThresholdTest {

  LongAdder irrelevant = new LongAdder();

  @ParameterizedTest(name = "[{index}] maxErrors {0} errorCount {1} = {2}")
  @CsvSource({
    "0,0,false",
    "0,1,true",
    "1,0,false",
    "1,1,false",
    "1,2,true",
    "100,99,false",
    "100,100,false",
    "100,101,true",
    "100,1000,true",
    "1000,100,false",
  })
  void should_check_threshold_exceeded(int maxErrors, int errorCount, boolean expected) {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(maxErrors);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(errorCount, irrelevant);
    // then
    assertThat(exceeded).isEqualTo(expected);
  }

  @ParameterizedTest(name = "[{index}] maxErrors {0}")
  @ValueSource(ints = {-1, -2, -100, Integer.MIN_VALUE})
  void should_error_out_when_max_errors_invalid(int maxErrors) {
    assertThatThrownBy(() -> new AbsoluteErrorThreshold(maxErrors))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrors must be >= 0");
  }

  @ParameterizedTest(name = "[{index}] maxErrors {0}")
  @ValueSource(ints = {0, 1, 2, 100, Integer.MAX_VALUE})
  void should_report_max_errors(int maxErrors) {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(maxErrors);
    // when
    long actual = threshold.getMaxErrors();
    // then
    assertThat(actual).isEqualTo(maxErrors);
  }

  @ParameterizedTest(name = "[{index}] maxErrorRatio {0} expected {1}")
  @ValueSource(ints = {0, 1, 2, 100, Integer.MAX_VALUE})
  void should_report_threshold_as_string(int maxErrors) {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(maxErrors);
    // when
    String actual = threshold.thresholdAsString();
    // then
    assertThat(actual).isEqualTo(Integer.toString(maxErrors));
  }
}
