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

class RatioErrorThresholdTest {

  @Test
  void should_return_false_when_errors_below_ratio() {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, 100);
    LongAdder total = new LongAdder();
    total.add(100);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(10, total);
    // then
    assertThat(exceeded).isFalse();
  }

  @Test
  void should_return_false_when_total_below_min_sample() {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, 100);
    LongAdder total = new LongAdder();
    total.add(99);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(99, total);
    // then
    assertThat(exceeded).isFalse();
  }

  @Test
  void should_return_true_when_errors_above_ratio_and_total_above_min_sample() {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, 100);
    LongAdder total = new LongAdder();
    total.add(100);
    // when
    boolean exceeded = threshold.checkThresholdExceeded(11, total);
    // then
    assertThat(exceeded).isTrue();
  }

  @Test
  void should_error_out_when_max_error_ratio_invalid() {
    assertThatThrownBy(() -> new RatioErrorThreshold(-1f, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrorRatio must be > 0 and < 1");
    assertThatThrownBy(() -> new RatioErrorThreshold(0f, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrorRatio must be > 0 and < 1");
    assertThatThrownBy(() -> new RatioErrorThreshold(1f, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrorRatio must be > 0 and < 1");
    assertThatThrownBy(() -> new RatioErrorThreshold(1000f, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxErrorRatio must be > 0 and < 1");
  }

  @Test
  void should_error_out_when_min_sample_invalid() {
    assertThatThrownBy(() -> new RatioErrorThreshold(0.1f, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minSample must be >= 1");
    assertThatThrownBy(() -> new RatioErrorThreshold(0.1f, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minSample must be >= 1");
  }

  @Test
  void should_report_max_error_ratio() {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, 100);
    // when
    float ratio = threshold.getMaxErrorRatio();
    // then
    assertThat(ratio).isEqualTo(0.1f);
  }

  @Test
  void should_report_min_sample() {
    // given
    RatioErrorThreshold threshold = new RatioErrorThreshold(0.1f, 100);
    // when
    long minSample = threshold.getMinSample();
    // then
    assertThat(minSample).isEqualTo(100L);
  }
}
