/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
import org.slf4j.LoggerFactory;

public class RecordReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReporter.class);

  private final String msg;
  private final long expectedTotal;

  public RecordReporter(
      MetricRegistry registry,
      TimeUnit rateUnit,
      ScheduledExecutorService scheduler,
      long expectedTotal) {
    super(registry, "record-reporter", createFilter(), rateUnit, TimeUnit.MILLISECONDS, scheduler);
    this.expectedTotal = expectedTotal;
    if (expectedTotal < 0) {
      msg = "Records: total: %,d, successful: %,d, failed: %,d, mean: %,.0f records/%s";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      msg =
          "Records: total: %,"
              + numDigits
              + "d, successful: %,"
              + numDigits
              + "d, failed: %,d, progression: %,.0f%%, mean: %,.0f records/%s";
    }
  }

  private static MetricFilter createFilter() {
    return (name, metric) ->
        name.equals("records/total")
            || name.equals("records/successful")
            || name.equals("records/failed");
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    Meter totalMeter = meters.get("records/total");
    Meter successfulMeter = meters.get("records/successful");
    Meter failedMeter = meters.get("records/failed");
    if (expectedTotal < 0) {
      reportWithoutExpectedTotal(totalMeter, successfulMeter, failedMeter);
    } else {
      reportWithExpectedTotal(totalMeter, successfulMeter, failedMeter);
    }
  }

  private void reportWithoutExpectedTotal(
      Meter totalMeter, Meter successfulMeter, Meter failedMeter) {
    LOGGER.info(
        String.format(
            msg,
            totalMeter.getCount(),
            successfulMeter.getCount(),
            failedMeter.getCount(),
            convertRate(totalMeter.getOneMinuteRate()),
            getRateUnit()));
  }

  private void reportWithExpectedTotal(Meter totalMeter, Meter successfulMeter, Meter failedMeter) {
    float progression = (float) totalMeter.getCount() / (float) expectedTotal * 100f;
    LOGGER.info(
        String.format(
            msg,
            totalMeter.getCount(),
            successfulMeter.getCount(),
            failedMeter.getCount(),
            progression,
            convertRate(totalMeter.getOneMinuteRate()),
            getRateUnit()));
  }
}
