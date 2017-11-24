/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class BatchReporterTest {

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(@LogCapture(BatchReporter.class) LogInterceptor interceptor)
      throws Exception {
    Histogram size = registry.histogram("batches/size");
    BatchReporter reporter =
        new BatchReporter(registry, Executors.newSingleThreadScheduledExecutor());
    reporter.report();
    assertThat(interceptor).hasMessageMatching("Batches: total: 0, size: 0.00 mean, 0 min, 0 max");
    size.update(2);
    size.update(1);
    size.update(1);
    size.update(1);
    reporter.report();
    assertThat(interceptor).hasMessageMatching("Batches: total: 4, size: 1.25 mean, 1 min, 2 max");
  }
}
