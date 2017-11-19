/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.UnmappableRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsReportingExecutionListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/** */
public class MetricsManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);

  private final MetricRegistry registry = new MetricRegistry();
  private final MetricsCollectingExecutionListener listener =
      new MetricsCollectingExecutionListener(registry);

  private final WorkflowType workflowType;
  private final String executionId;
  private final ScheduledExecutorService scheduler;
  private final TimeUnit rateUnit;
  private final TimeUnit durationUnit;
  private final long expectedWrites;
  private final long expectedReads;
  private final boolean jmx;
  private final boolean csv;
  private final Path executionDirectory;
  private final Duration reportInterval;
  private final boolean batchingEnabled;

  private Meter records;
  private Meter failedRecords;
  private Meter successfulRecords;
  private Meter mappings;
  private Meter failedMappings;
  private Meter successfulMappings;
  private Histogram batchSize;
  private RecordReporter resultMappingsReporter;
  private MappingReporter recordMappingsReporter;
  private BatchReporter batchesReporter;
  private MemoryReporter memoryReporter;
  private MetricsReportingExecutionListener writesReporter;
  private MetricsReportingExecutionListener readsReporter;
  private JmxReporter jmxReporter;
  private CsvReporter csvReporter;

  public MetricsManager(
      WorkflowType workflowType,
      String executionId,
      ScheduledExecutorService scheduler,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedWrites,
      long expectedReads,
      boolean jmx,
      boolean csv,
      Path executionDirectory,
      Duration reportInterval,
      boolean batchingEnabled) {
    this.workflowType = workflowType;
    this.executionId = executionId;
    this.scheduler = scheduler;
    this.rateUnit = rateUnit;
    this.durationUnit = durationUnit;
    this.expectedWrites = expectedWrites;
    this.expectedReads = expectedReads;
    this.jmx = jmx;
    this.csv = csv;
    this.executionDirectory = executionDirectory;
    this.reportInterval = reportInterval;
    this.batchingEnabled = batchingEnabled;
  }

  public void init() {
    records = registry.meter("records/total");
    successfulRecords = registry.meter("records/successful");
    failedRecords = registry.meter("records/failed");
    mappings = registry.meter("mappings/total");
    failedMappings = registry.meter("mappings/failed");
    successfulMappings = registry.meter("mappings/successful");
    batchSize = registry.histogram("batches/size", () -> new Histogram(new UniformReservoir()));
    createMemoryGauges();

    if (jmx) {
      startJMXReporter();
    }
    if (csv) {
      startCSVReporter();
    }

    startMemoryReporter();
    switch (workflowType) {
      case LOAD:
        startRecordMappingsReporter();
        if (batchingEnabled) {
          startBatchesReporter();
        }
        startWritesReporter();
        break;

      case UNLOAD:
        startReadsReporter();
        startResultMappingsReporter();
        break;
    }
  }

  private void createMemoryGauges() {
    final int bytesPerMeg = 1024 * 1024;
    registry.gauge(
        "memory/used",
        () ->
            () -> {
              Runtime runtime = Runtime.getRuntime();
              return (runtime.totalMemory() - runtime.freeMemory()) / bytesPerMeg;
            });

    registry.gauge(
        "memory/free",
        () ->
            () -> {
              Runtime runtime = Runtime.getRuntime();
              return runtime.freeMemory() / bytesPerMeg;
            });

    registry.gauge(
        "memory/allocated",
        () ->
            () -> {
              Runtime runtime = Runtime.getRuntime();
              return runtime.totalMemory() / bytesPerMeg;
            });

    registry.gauge(
        "memory/available",
        () ->
            () -> {
              Runtime runtime = Runtime.getRuntime();
              return runtime.maxMemory() / bytesPerMeg;
            });

    registry.gauge(
        "memory/gc_count",
        () ->
            () -> {
              // GC stats logic is from https://stackoverflow.com/a/467366/1786686
              long total = 0;

              for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long count = gc.getCollectionCount();
                if (count >= 0) {
                  total += count;
                }
              }
              return total;
            });

    registry.gauge(
        "memory/gc_time",
        () ->
            () -> {
              // GC stats logic is from https://stackoverflow.com/a/467366/1786686
              long gcTime = 0;

              for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long time = gc.getCollectionTime();
                if (time >= 0) {
                  gcTime += time;
                }
              }
              return gcTime;
            });
  }

  private void startJMXReporter() {
    jmxReporter =
        JmxReporter.forRegistry(registry)
            .convertDurationsTo(durationUnit)
            .convertRatesTo(rateUnit)
            .createsObjectNamesWith(
                (type, domain, name) -> {
                  try {
                    StringBuilder sb =
                        new StringBuilder("com.datastax.dsbulk:0=").append(executionId).append(',');
                    StringTokenizer tokenizer = new StringTokenizer(name, "/");
                    int i = 1;
                    while (tokenizer.hasMoreTokens()) {
                      String token = tokenizer.nextToken();
                      if (tokenizer.hasMoreTokens()) {
                        sb.append(i++).append('=').append(token).append(',');
                      } else {
                        sb.append("name=").append(token);
                      }
                    }
                    return new ObjectName(sb.toString());
                  } catch (MalformedObjectNameException e) {
                    throw new RuntimeException(e);
                  }
                })
            .build();
    jmxReporter.start();
  }

  private void startCSVReporter() {
    csvReporter =
        CsvReporter.forRegistry(registry)
            .convertDurationsTo(durationUnit)
            .convertRatesTo(rateUnit)
            .build(executionDirectory.toFile());
    csvReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startResultMappingsReporter() {
    resultMappingsReporter = new RecordReporter(registry, rateUnit, scheduler, expectedWrites);
    resultMappingsReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startRecordMappingsReporter() {
    recordMappingsReporter = new MappingReporter(registry, rateUnit, scheduler, expectedWrites);
    recordMappingsReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startBatchesReporter() {
    batchesReporter = new BatchReporter(registry, scheduler);
    batchesReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startMemoryReporter() {
    memoryReporter = new MemoryReporter(registry, scheduler);
    memoryReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startWritesReporter() {
    MetricsReportingExecutionListener.Builder builder =
        MetricsReportingExecutionListener.builder()
            .reportingWrites()
            .withScheduler(scheduler)
            .convertRatesTo(rateUnit)
            .convertDurationsTo(durationUnit)
            .extractingMetricsFrom(listener);
    if (expectedWrites > 0) {
      builder.expectingTotalEvents(expectedWrites);
    }
    writesReporter = builder.build();
    writesReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startReadsReporter() {
    MetricsReportingExecutionListener.Builder builder =
        MetricsReportingExecutionListener.builder()
            .reportingReads()
            .withScheduler(scheduler)
            .convertRatesTo(rateUnit)
            .convertDurationsTo(durationUnit)
            .extractingMetricsFrom(listener);
    if (expectedReads > 0) {
      builder.expectingTotalEvents(expectedWrites);
    }
    readsReporter = builder.build();
    readsReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  @Override
  public void close() throws Exception {
    closeReporters();
    scheduler.shutdown();
    scheduler.awaitTermination(1, MINUTES);
    scheduler.shutdownNow();
  }

  public void reportFinalMetrics() {
    LOGGER.info("Final stats:");
    if (resultMappingsReporter != null) {
      resultMappingsReporter.report();
    }
    if (recordMappingsReporter != null) {
      recordMappingsReporter.report();
    }
    if (batchesReporter != null) {
      batchesReporter.report();
    }
    if (memoryReporter != null) {
      memoryReporter.report();
    }
    if (writesReporter != null) {
      writesReporter.report();
    }
    if (readsReporter != null) {
      readsReporter.report();
    }
  }

  private void closeReporters() {
    if (jmxReporter != null) {
      jmxReporter.close();
    }
    if (csvReporter != null) {
      csvReporter.close();
    }
    if (resultMappingsReporter != null) {
      resultMappingsReporter.close();
    }
    if (recordMappingsReporter != null) {
      recordMappingsReporter.close();
    }
    if (batchesReporter != null) {
      batchesReporter.close();
    }
    if (memoryReporter != null) {
      memoryReporter.close();
    }
    if (writesReporter != null) {
      writesReporter.close();
    }
    if (readsReporter != null) {
      readsReporter.close();
    }
  }

  public Function<Flux<Record>, Flux<Record>> newUnmappableRecordMonitor() {
    return upstream ->
        upstream.doOnNext(
            r -> {
              records.mark();
              if (r instanceof UnmappableRecord) {
                failedRecords.mark();
              } else {
                successfulRecords.mark();
              }
            });
  }

  public Function<Flux<Statement>, Flux<Statement>> newUnmappableStatementMonitor() {
    return upstream ->
        upstream.doOnNext(
            stmt -> {
              mappings.mark();
              if (stmt instanceof UnmappableStatement) {
                failedMappings.mark();
              } else {
                successfulMappings.mark();
              }
            });
  }

  public Function<Flux<Statement>, Flux<Statement>> newBatcherMonitor() {
    return upstream ->
        upstream.doOnNext(
            stmt -> {
              if (stmt instanceof BatchStatement) {
                batchSize.update(((BatchStatement) stmt).size());
              } else {
                batchSize.update(1);
              }
            });
  }

  public MetricsCollectingExecutionListener getExecutionListener() {
    return listener;
  }
}
