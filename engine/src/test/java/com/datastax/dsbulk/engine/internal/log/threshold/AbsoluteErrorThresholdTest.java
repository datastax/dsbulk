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
import org.junit.jupiter.api.Test;

class AbsoluteErrorThresholdTest {

  private LongAdder irrelevant = new LongAdder();

  @Test
  void should_return_false_when_errors_below_threshold() {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(100);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(100, irrelevant);
    // then
    assertThat(exceeded).isFalse();
  }

  @Test
  void should_return_true_when_errors_above_threshold() {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(100);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(101, irrelevant);
    // then
    assertThat(exceeded).isTrue();
  }

  @Test
  void should_error_out_when_max_errors_invalid() {
    assertThatThrownBy(() -> new AbsoluteErrorThreshold(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrors must be >= 0");
  }

  @Test
  void should_report_max_errors() {
    // given
    AbsoluteErrorThreshold threshold = new AbsoluteErrorThreshold(100);
    // when
    long maxErrors = threshold.getMaxErrors();
    // then
    assertThat(maxErrors).isEqualTo(100);
  }
}
