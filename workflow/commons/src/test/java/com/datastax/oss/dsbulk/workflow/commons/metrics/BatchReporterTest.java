/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.metrics;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.slf4j.event.Level.DEBUG;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.dsbulk.executor.api.listener.LogSink;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(LogInterceptingExtension.class)
class BatchReporterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchReporter.class);

  private MetricRegistry registry = new MetricRegistry();

  @Test
  void should_report_batches(
      @LogCapture(value = BatchReporter.class, level = DEBUG) LogInterceptor interceptor) {
    Histogram size = registry.histogram("batches/size");
    LogSink sink = LogSink.buildFrom(LOGGER::isDebugEnabled, LOGGER::debug);
    BatchReporter reporter =
        new BatchReporter(registry, sink, Executors.newSingleThreadScheduledExecutor());
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
