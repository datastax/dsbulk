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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryReporter.class);

  private static final String MSG =
      "Memory usage: used: %,d MB, free: %,d MB, total garbage collections: %,d, total gc time: %,d ms";

  public MemoryReporter(MetricRegistry registry, ScheduledExecutorService scheduler) {
    super(registry, "memory-reporter", createFilter(), SECONDS, MILLISECONDS, scheduler);
  }

  private static MetricFilter createFilter() {
    return (name, metric) -> false;
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    // GC stats logic is from https://stackoverflow.com/a/467366/1786686
    long totalGarbageCollections = 0;
    long garbageCollectionTime = 0;

    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long count = gc.getCollectionCount();

      if (count >= 0) {
        totalGarbageCollections += count;
      }

      long time = gc.getCollectionTime();

      if (time >= 0) {
        garbageCollectionTime += time;
      }
    }

    Runtime runtime = Runtime.getRuntime();
    LOGGER.info(
        String.format(
            MSG,
            (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024),
            runtime.freeMemory() / (1024 * 1024),
            totalGarbageCollections,
            garbageCollectionTime));
  }
}
