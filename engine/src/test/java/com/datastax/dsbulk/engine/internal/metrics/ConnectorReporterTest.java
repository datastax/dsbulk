/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class ConnectorReporterTest {

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(@LogCapture(RecordReporter.class) LogInterceptor interceptor)
      throws Exception {
    Meter totalMeter = registry.meter("records/total");
    Meter successfulMeter = registry.meter("records/successful");
    Meter failedMeter = registry.meter("records/failed");
    RecordReporter reporter =
        new RecordReporter(registry, SECONDS, Executors.newSingleThreadScheduledExecutor(), -1);
    reporter.report();
    assertThat(interceptor)
        .hasMessageMatching("Records: total: 0, successful: 0, failed: 0, mean: 0 records/second");
    totalMeter.mark(3);
    successfulMeter.mark(2);
    failedMeter.mark(1);
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor).hasMessageMatching("Records: total: 3, successful: 2, failed: 1");
  }
}
