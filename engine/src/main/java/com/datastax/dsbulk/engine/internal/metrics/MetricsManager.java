/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsReportingExecutionListener;
import com.datastax.dsbulk.executor.api.result.Result;
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

  private final MetricRegistry registry;
  private final MetricsCollectingExecutionListener listener;

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

  private Meter totalRecords;
  private Counter failedRecords;
  private Histogram batchSize;
  private RecordReporter recordReporter;
  private BatchReporter batchesReporter;
  private MemoryReporter memoryReporter;
  private MetricsReportingExecutionListener writesReporter;
  private MetricsReportingExecutionListener readsReporter;
  private JmxReporter jmxReporter;
  private CsvReporter csvReporter;

  public MetricsManager(
      MetricRegistry driverRegistry,
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
    this.registry = new MetricRegistry();
    driverRegistry
        .getMetrics()
        .forEach((name, metric) -> this.registry.register("driver/" + name, metric));
    this.listener = new MetricsCollectingExecutionListener(registry);
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
    totalRecords = registry.meter("records/total");
    failedRecords = registry.counter("records/failed");
    batchSize = registry.histogram("batches/size", () -> new Histogram(new UniformReservoir()));
    createMemoryGauges();

    if (jmx) {
      startJMXReporter();
    }
    if (csv) {
      startCSVReporter();
    }

    startMemoryReporter();
    startRecordReporter();
    switch (workflowType) {
      case LOAD:
        if (batchingEnabled) {
          startBatchesReporter();
        }
        startWritesReporter();
        break;

      case UNLOAD:
        startReadsReporter();
        break;
    }
  }

  private void createMemoryGauges() {
    long bytesPerMeg = 1024 * 1024;
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

  private void startRecordReporter() {
    recordReporter = new RecordReporter(registry, rateUnit, scheduler, expectedWrites);
    recordReporter.start(reportInterval.getSeconds(), SECONDS);
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
    if (recordReporter != null) {
      recordReporter.report();
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
    if (recordReporter != null) {
      recordReporter.close();
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

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsMonitor() {
    return upstream -> upstream.doOnNext(item -> totalRecords.mark());
  }

  public <T> Function<Flux<T>, Flux<T>> newFailedItemsMonitor() {
    return upstream ->
        upstream.doOnNext(
            item -> {
              if (item instanceof ErrorRecord
                  || item instanceof UnmappableStatement
                  || (item instanceof Result && !((Result) item).isSuccess())) {
                failedRecords.inc();
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
