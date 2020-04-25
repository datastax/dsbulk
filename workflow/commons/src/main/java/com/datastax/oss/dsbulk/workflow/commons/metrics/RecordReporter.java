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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.datastax.oss.dsbulk.executor.api.listener.LogSink;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RecordReporter extends ScheduledReporter {

  private final String msg;
  private final long expectedTotal;
  private final LogSink sink;

  RecordReporter(
      MetricRegistry registry,
      LogSink sink,
      TimeUnit rateUnit,
      ScheduledExecutorService scheduler,
      long expectedTotal) {
    super(registry, "record-reporter", createFilter(), rateUnit, TimeUnit.MILLISECONDS, scheduler);
    this.sink = sink;
    this.expectedTotal = expectedTotal;
    if (expectedTotal < 0) {
      msg = "Records: total: %,d, successful: %,d, failed: %,d";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      msg =
          "Records: total: %,"
              + numDigits
              + "d, successful: %,"
              + numDigits
              + "d, failed: %,d, progression: %,.0f%%";
    }
  }

  private static MetricFilter createFilter() {
    return (name, metric) -> name.equals("records/total") || name.equals("records/failed");
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
    Counter totalCounter = counters.get("records/total");
    Counter failedCounter = counters.get("records/failed");
    if (expectedTotal < 0) {
      reportWithoutExpectedTotal(totalCounter, failedCounter);
    } else {
      reportWithExpectedTotal(totalCounter, failedCounter);
    }
  }

  private void reportWithoutExpectedTotal(Counter totalCounter, Counter failedCounter) {
    long total = totalCounter.getCount();
    long failed = failedCounter.getCount();
    sink.accept(String.format(msg, total, total - failed, failed));
  }

  private void reportWithExpectedTotal(Counter totalCounter, Counter failedCounter) {
    long total = totalCounter.getCount();
    long failed = failedCounter.getCount();
    float progression = (float) total / (float) expectedTotal * 100f;
    sink.accept(String.format(msg, total, total - failed, failed, progression));
  }
}
