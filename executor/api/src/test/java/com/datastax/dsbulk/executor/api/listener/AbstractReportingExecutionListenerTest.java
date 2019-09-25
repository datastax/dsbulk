/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
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
