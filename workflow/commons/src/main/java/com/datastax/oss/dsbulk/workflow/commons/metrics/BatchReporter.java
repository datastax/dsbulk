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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.oss.dsbulk.executor.api.listener.LogSink;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;

public class BatchReporter extends ScheduledReporter {

  private static final String MSG = "Batches: total: %,d, size: %,.2f mean, %d min, %d max";

  private final LogSink sink;

  BatchReporter(MetricRegistry registry, LogSink sink, ScheduledExecutorService scheduler) {
    super(registry, "batch-reporter", createFilter(), SECONDS, MILLISECONDS, scheduler);
    this.sink = sink;
  }

  private static MetricFilter createFilter() {
    return (name, metric) -> name.equals("batches/size");
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    if (!sink.isEnabled()) {
      return;
    }
    Histogram size = histograms.get("batches/size");
    Snapshot snapshot = size.getSnapshot();
    sink.accept(
        String.format(
            MSG, size.getCount(), snapshot.getMean(), snapshot.getMin(), snapshot.getMax()));
  }
}
