/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class RecordReporter extends ScheduledReporter {

  private final String msg;
  private final long expectedTotal;
  private final Logger logger;

  RecordReporter(
      MetricRegistry registry,
      Logger logger,
      TimeUnit rateUnit,
      ScheduledExecutorService scheduler,
      long expectedTotal) {
    super(registry, "record-reporter", createFilter(), rateUnit, TimeUnit.MILLISECONDS, scheduler);
    this.logger = logger;
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
    logger.info(String.format(msg, total, total - failed, failed));
  }

  private void reportWithExpectedTotal(Counter totalCounter, Counter failedCounter) {
    long total = totalCounter.getCount();
    long failed = failedCounter.getCount();
    float progression = (float) total / (float) expectedTotal * 100f;
    logger.info(String.format(msg, total, total - failed, failed, progression));
  }
}
