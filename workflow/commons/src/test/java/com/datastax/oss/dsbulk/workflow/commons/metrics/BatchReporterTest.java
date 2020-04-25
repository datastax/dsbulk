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
