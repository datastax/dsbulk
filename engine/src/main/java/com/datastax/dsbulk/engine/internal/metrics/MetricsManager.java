/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.Verbosity.normal;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.Verbosity.quiet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.commons.log.LogSink;
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.listener.AbstractMetricsReportingExecutionListenerBuilder;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.listener.ReadsReportingExecutionListener;
import com.datastax.dsbulk.executor.api.listener.WritesReportingExecutionListener;
import com.datastax.dsbulk.executor.api.result.Result;
import com.google.common.util.concurrent.MoreExecutors;
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
import org.slf4j.Marker;
import org.slf4j.helpers.BasicMarkerFactory;
import reactor.core.publisher.Flux;

public class MetricsManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManager.class);
  private static final Marker METRICS_MARKER = new BasicMarkerFactory().getMarker("METRICS");

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
  private final LogSettings.Verbosity verbosity;

  private Counter totalRecords;
  private Counter failedRecords;
  private Histogram batchSize;
  private RecordReporter recordReporter;
  private BatchReporter batchesReporter;
  private MemoryReporter memoryReporter;
  private WritesReportingExecutionListener writesReporter;
  private ReadsReportingExecutionListener readsReporter;
  private JmxReporter jmxReporter;
  private CsvReporter csvReporter;
  private ConsoleReporter consoleReporter;
  private LogSink logSink;

  private volatile boolean running;

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
      LogSettings.Verbosity verbosity,
      Duration reportInterval,
      boolean batchingEnabled,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry) {
    this.registry = new MetricRegistry();
    driverRegistry
        .getMetrics()
        .forEach((name, metric) -> this.registry.register("driver/" + name, metric));
    this.listener =
        new MetricsCollectingExecutionListener(registry, protocolVersion, codecRegistry);
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
    this.verbosity = verbosity;
    this.reportInterval = reportInterval;
    this.batchingEnabled = batchingEnabled;
  }

  public void init() {
    totalRecords = registry.counter("records/total");
    failedRecords = registry.counter("records/failed");
    batchSize = registry.histogram("batches/size", () -> new Histogram(new UniformReservoir()));
    createMemoryGauges();

    if (jmx) {
      startJMXReporter();
    }
    if (csv) {
      startCSVReporter();
    }

    running = true;
    logSink =
        new LogSink() {

          @Override
          public boolean isEnabled() {
            return running ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled();
          }

          @Override
          public void accept(String message, Object... args) {
            if (running) {
              LOGGER.debug(METRICS_MARKER, message, args);
            } else {
              LOGGER.info(METRICS_MARKER, message, args);
            }
          }
        };

    if (verbosity.compareTo(quiet) > 0) {
      startConsoleReporter();
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
        case COUNT:
          startReadsReporter();
          break;
      }
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
    recordReporter = new RecordReporter(registry, logSink, rateUnit, scheduler, expectedWrites);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(normal) > 0) {
      recordReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startBatchesReporter() {
    batchesReporter = new BatchReporter(registry, logSink, scheduler);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(normal) > 0) {
      batchesReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startMemoryReporter() {
    memoryReporter = new MemoryReporter(registry, logSink, scheduler);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(normal) > 0) {
      memoryReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startWritesReporter() {
    AbstractMetricsReportingExecutionListenerBuilder<WritesReportingExecutionListener> builder =
        WritesReportingExecutionListener.builder()
            .withScheduler(scheduler)
            .withLogSink(logSink)
            .convertRatesTo(rateUnit)
            .convertDurationsTo(durationUnit)
            .extractingMetricsFrom(listener);
    if (expectedWrites > 0) {
      builder.expectingTotalEvents(expectedWrites);
    }
    writesReporter = builder.build();
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(normal) > 0) {
      writesReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startReadsReporter() {
    AbstractMetricsReportingExecutionListenerBuilder<ReadsReportingExecutionListener> builder =
        ReadsReportingExecutionListener.builder()
            .withScheduler(scheduler)
            .withLogSink(logSink)
            .convertRatesTo(rateUnit)
            .convertDurationsTo(durationUnit)
            .extractingMetricsFrom(listener);
    if (expectedReads > 0) {
      builder.expectingTotalEvents(expectedReads);
    }
    readsReporter = builder.build();
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(normal) > 0) {
      readsReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startConsoleReporter() {
    if (workflowType == LOAD) {
      registry.counter("records/failed");
      consoleReporter =
          new ConsoleReporter(
              registry,
              () -> totalRecords.getCount(),
              () -> failedRecords.getCount() + listener.getFailedWritesCounter().getCount(),
              listener.getTotalWritesTimer(),
              listener.getBytesSentMeter(),
              registry.histogram("batches/size"),
              SECONDS,
              MILLISECONDS,
              expectedWrites,
              scheduler);
    } else {
      consoleReporter =
          new ConsoleReporter(
              registry,
              () -> listener.getTotalReadsTimer().getCount(),
              () -> failedRecords.getCount() + listener.getFailedReadsCounter().getCount(),
              listener.getTotalReadsTimer(),
              listener.getBytesReceivedMeter(),
              null,
              SECONDS,
              MILLISECONDS,
              expectedReads,
              scheduler);
    }
    consoleReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  @Override
  public void close() {
    running = false;
    stopProgress();
    closeReporters();
    MoreExecutors.shutdownAndAwaitTermination(scheduler, 1, MINUTES);
  }

  public void stopProgress() {
    if (consoleReporter != null) {
      consoleReporter.report();
      consoleReporter.close();
      consoleReporter = null;
    }
  }

  public void reportFinalMetrics() {
    LOGGER.info(METRICS_MARKER, "Final stats:");
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
    return upstream -> upstream.doOnNext(item -> totalRecords.inc());
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
