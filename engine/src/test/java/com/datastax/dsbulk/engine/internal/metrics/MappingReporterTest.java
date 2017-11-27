/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class MappingReporterTest {

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(@LogCapture(MappingReporter.class) LogInterceptor interceptor)
      throws Exception {
    Meter totalMeter = registry.meter("mappings/total");
    Meter successfulMeter = registry.meter("mappings/successful");
    Meter failedMeter = registry.meter("mappings/failed");
    MappingReporter reporter =
        new MappingReporter(registry, SECONDS, Executors.newSingleThreadScheduledExecutor(), -1);
    reporter.report();
    assertThat(interceptor)
        .hasMessageMatching(
            "Mappings: total: 0, successful: 0, failed: 0, mean: 0 mappings/second");
    totalMeter.mark(3);
    successfulMeter.mark(2);
    failedMeter.mark(1);
    reporter.report();
    // can't assert mean rate as it may vary
    assertThat(interceptor).hasMessageMatching("Mappings: total: 3, successful: 2, failed: 1");
  }
}
