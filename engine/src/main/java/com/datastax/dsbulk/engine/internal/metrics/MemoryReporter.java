/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
import com.codahale.metrics.Timer;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;

public class MemoryReporter extends ScheduledReporter {

  private static final String MSG =
      "Memory usage: used: %,d MB, free: %,d MB, allocated: %,d MB, available: %,d MB, "
          + "total gc count: %,d, total gc time: %,d ms";

  private final Logger logger;

  MemoryReporter(MetricRegistry registry, Logger logger, ScheduledExecutorService scheduler) {
    super(registry, "memory-reporter", createFilter(), SECONDS, MILLISECONDS, scheduler);
    this.logger = logger;
  }

  private static MetricFilter createFilter() {
    return (name, metric) ->
        name.equals("memory/used")
            || name.equals("memory/free")
            || name.equals("memory/allocated")
            || name.equals("memory/available")
            || name.equals("memory/gc_count")
            || name.equals("memory/gc_time");
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    Gauge<?> freeMemoryGauge = gauges.get("memory/free");
    Gauge<?> allocatedMemoryGauge = gauges.get("memory/allocated");
    Gauge<?> usedMemoryGauge = gauges.get("memory/used");
    Gauge<?> availableMemoryGauge = gauges.get("memory/available");
    Gauge<?> gcCountGauge = gauges.get("memory/gc_count");
    Gauge<?> gcTimeGauge = gauges.get("memory/gc_time");
    long usedMemory = (Long) usedMemoryGauge.getValue();
    long freeMemory = (Long) freeMemoryGauge.getValue();
    long allocatedMemory = (Long) allocatedMemoryGauge.getValue();
    long availableMemory = (Long) availableMemoryGauge.getValue();
    long gcCount = (Long) gcCountGauge.getValue();
    long gcTime = (Long) gcTimeGauge.getValue();
    logger.info(
        String.format(
            MSG, usedMemory, freeMemory, allocatedMemory, availableMemory, gcCount, gcTime));
  }
}
