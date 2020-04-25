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

class RatioErrorThresholdTest {

  @ParameterizedTest(
      name = "[{index}] maxErrorRatio {0} minSample {1} totalItems {2} errorCount {3} = {4}")
  @CsvSource({
    //  %   min   tot   err   exc
    "0.1  ,   1 ,   1 ,   0 , false",
    "0.1  ,   1 ,   1 ,   1 ,  true",
    "0.1  , 100 ,  99 ,  99 , false", // minSample not met
    "0.1  , 100 , 100 ,  10 , false",
    "0.1  , 100 , 100 ,  11 ,  true",
    "0.5  ,  10 ,  10 ,   4 , false",
    "0.5  ,  10 ,  10 ,   5 , false",
    "0.5  ,  10 ,  10 ,   6 ,  true",
    "0.5  ,  11 ,  10 ,   6 , false", // minSample not met
    "0.99 , 100 , 100 ,  98 , false",
    "0.99 , 100 , 100 ,  99 , false",
    "0.99 , 100 , 100 , 100 ,  true",
  })
  void should_check_threshold_exceeded(
      float maxErrorRatio, long minSample, long totalItems, int errorCount, boolean expected) {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(maxErrorRatio, minSample);
    LongAdder total = new LongAdder();
    total.add(totalItems);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(errorCount, total);
    // then
    assertThat(exceeded).isEqualTo(expected);
  }

  @ParameterizedTest(name = "[{index}] maxErrorRatio {0}")
  @ValueSource(floats = {-1, 0, 1, 100})
  void should_error_out_when_max_error_ratio_invalid(float maxErrorRatio) {
    assertThatThrownBy(() -> new RatioErrorThreshold(maxErrorRatio, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrorRatio must be > 0 and < 1");
  }

  @ParameterizedTest(name = "[{index}] minSample {0}")
  @ValueSource(longs = {Long.MIN_VALUE, -100, -1, 0})
  void should_error_out_when_min_sample_invalid(long minSample) {
    assertThatThrownBy(() -> new RatioErrorThreshold(0.1f, minSample))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minSample must be >= 1");
  }

  @ParameterizedTest(name = "[{index}] maxErrorRatio {0}")
  @ValueSource(floats = {0.001f, 0.1f, 0.5f, 0.9f, 0.999f})
  void should_report_max_error_ratio(float maxErrorRatio) {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(maxErrorRatio, 100);
    // when
    float actual = threshold.getMaxErrorRatio();
    // then
    assertThat(actual).isEqualTo(maxErrorRatio);
  }

  @ParameterizedTest(name = "[{index}] maxErrorRatio {0} expected {1}")
  @CsvSource({"0.001,0.1%", "0.1,10%", "0.5,50%", "0.9f,90%", "0.999,99.9%"})
  void should_report_threshold_as_string(float maxErrorRatio, String expected) {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(maxErrorRatio, 100);
    // when
    String actual = threshold.thresholdAsString();
    // then
    assertThat(actual).isEqualTo(expected);
  }

  @ParameterizedTest(name = "[{index}] minSample {0}")
  @ValueSource(longs = {1, 2, 10, 100, Long.MAX_VALUE})
  void should_report_min_sample(long minSample) {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, minSample);
    // when
    long actual = threshold.getMinSample();
    // then
    assertThat(actual).isEqualTo(minSample);
  }
}
