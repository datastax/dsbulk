/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.threshold;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AbsoluteErrorThresholdTest {

  private LongAdder irrelevant = new LongAdder();

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
}
