/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

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
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchReporter.class);

  private static final String MSG = "Batches: total: %,d, size: %,.2f mean, %d min, %d max";

  public BatchReporter(MetricRegistry registry, ScheduledExecutorService scheduler) {
    super(registry, "batch-reporter", createFilter(), SECONDS, MILLISECONDS, scheduler);
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
    Histogram size = histograms.get("batches/size");
    Snapshot snapshot = size.getSnapshot();
    LOGGER.info(
        String.format(
            MSG, size.getCount(), snapshot.getMean(), snapshot.getMin(), snapshot.getMax()));
  }
}
