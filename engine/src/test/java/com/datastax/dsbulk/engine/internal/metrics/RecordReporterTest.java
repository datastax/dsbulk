/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(LogInterceptingExtension.class)
class RecordReporterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReporter.class);

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(@LogCapture(RecordReporter.class) LogInterceptor interceptor) {
    Meter totalMeter = registry.meter("records/total");
    Counter failedMeter = registry.counter("records/failed");
    RecordReporter reporter =
        new RecordReporter(
            registry, LOGGER, SECONDS, Executors.newSingleThreadScheduledExecutor(), -1);
    reporter.report();
    assertThat(interceptor)
        .hasMessageContaining(
            "Records: total: 0, successful: 0, failed: 0, mean: 0 records/second");
    totalMeter.mark(3);
    failedMeter.inc();
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor).hasMessageContaining("Records: total: 3, successful: 2, failed: 1");
  }

  @Test
  void should_report_batches_with_expected_total(
      @LogCapture(RecordReporter.class) LogInterceptor interceptor) {
    Meter totalMeter = registry.meter("records/total");
    Counter failedMeter = registry.counter("records/failed");
    RecordReporter reporter =
        new RecordReporter(
            registry, LOGGER, SECONDS, Executors.newSingleThreadScheduledExecutor(), 3);
    reporter.report();
    assertThat(interceptor)
        .hasMessageContaining(
            "Records: total: 0, successful: 0, failed: 0, progression: 0%, mean: 0 records/second");
    totalMeter.mark(3);
    failedMeter.inc();
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor)
        .hasMessageContaining("Records: total: 3, successful: 2, failed: 1, progression: 100%");
  }
}
