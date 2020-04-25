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
package com.datastax.oss.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

abstract class AbstractReportingExecutionListenerTest {

  final LogInterceptor interceptor;

  @Mock MetricsCollectingExecutionListener delegate;

  @Mock Timer total;
  @Mock Counter successful;
  @Mock Counter failed;
  @Mock Counter inFlight;
  @Mock Meter bytesSent;
  @Mock Meter bytesReceived;
  @Mock Snapshot latencies;

  AbstractReportingExecutionListenerTest(LogInterceptor interceptor) {
    this.interceptor = interceptor;
  }

  @BeforeEach
  void setUpMetrics() {
    when(delegate.getRegistry()).thenReturn(new MetricRegistry());
    when(delegate.getInFlightRequestsCounter()).thenReturn(inFlight);
    when(total.getCount()).thenReturn(100_000L);
    when(total.getMeanRate()).thenReturn(1_000d);
    when(successful.getCount()).thenReturn(99_999L);
    when(failed.getCount()).thenReturn(1L);
    when(inFlight.getCount()).thenReturn(500L);
    when(total.getSnapshot()).thenReturn(latencies);
    when(latencies.getMean()).thenReturn((double) MILLISECONDS.toNanos(50));
    when(latencies.get99thPercentile()).thenReturn((double) MILLISECONDS.toNanos(100));
    when(latencies.get999thPercentile()).thenReturn((double) MILLISECONDS.toNanos(250));
  }
}
