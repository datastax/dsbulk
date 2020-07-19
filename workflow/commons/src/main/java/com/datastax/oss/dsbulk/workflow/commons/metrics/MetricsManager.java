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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.MoreExecutors;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.executor.api.listener.AbstractMetricsReportingExecutionListenerBuilder;
import com.datastax.oss.dsbulk.executor.api.listener.LogSink;
import com.datastax.oss.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.oss.dsbulk.executor.api.listener.ReadsReportingExecutionListener;
import com.datastax.oss.dsbulk.executor.api.listener.WritesReportingExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings.Verbosity;
import com.datastax.oss.dsbulk.workflow.commons.settings.RowType;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import com.datastax.oss.dsbulk.workflow.commons.utils.JMXUtils;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final String DSBULK_JMX_DOMAIN = "com.datastax.oss.dsbulk";

  private final MetricRegistry registry;
  private final MetricsCollectingExecutionListener listener;

  private final boolean monitorWrites;
  private final String executionId;
  private final ScheduledExecutorService scheduler;
  private final TimeUnit rateUnit;
  private final TimeUnit durationUnit;
  private final long expectedWrites;
  private final long expectedReads;
  private final boolean jmx;
  private final boolean csv;
  private final Path operationDirectory;
  private final Duration reportInterval;
  private final boolean batchingEnabled;
  private final Verbosity verbosity;
  private final RowType rowType;

  private Counter totalItems;
  private Counter failedItems;
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

  private final AtomicBoolean running = new AtomicBoolean(false);

  public MetricsManager(
      MetricRegistry driverRegistry,
      boolean monitorWrites,
      String executionId,
      ScheduledExecutorService scheduler,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedWrites,
      long expectedReads,
      boolean trackBytes,
      boolean jmx,
      boolean csv,
      Path operationDirectory,
      Verbosity verbosity,
      Duration reportInterval,
      boolean batchingEnabled,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      RowType rowType) {
    this.registry = new MetricRegistry();
    driverRegistry
        .getMetrics()
        .forEach((name, metric) -> this.registry.register("driver/" + name, metric));
    this.monitorWrites = monitorWrites;
    this.listener =
        new MetricsCollectingExecutionListener(
            registry, protocolVersion, codecRegistry, trackBytes);
    this.executionId = executionId;
    this.scheduler = scheduler;
    this.rateUnit = rateUnit;
    this.durationUnit = durationUnit;
    this.expectedWrites = expectedWrites;
    this.expectedReads = expectedReads;
    this.jmx = jmx;
    this.csv = csv;
    this.operationDirectory = operationDirectory;
    this.verbosity = verbosity;
    this.reportInterval = reportInterval;
    this.batchingEnabled = batchingEnabled;
    this.rowType = rowType;
  }

  public void init() {
    totalItems = registry.counter("records/total");
    failedItems = registry.counter("records/failed");
    batchSize = registry.histogram("batches/size", () -> new Histogram(new UniformReservoir()));
    createMemoryGauges();
    logSink =
        new LogSink() {

          @Override
          public boolean isEnabled() {
            return running.get() ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled();
          }

          @Override
          public void accept(String message, Object... args) {
            if (running.get()) {
              LOGGER.debug(METRICS_MARKER, message, args);
            } else {
              LOGGER.info(METRICS_MARKER, message, args);
            }
          }
        };
  }

  public void start() {
    running.set(true);
    if (jmx) {
      startJMXReporter();
    }
    if (csv) {
      startCSVReporter();
    }
    if (verbosity.compareTo(Verbosity.quiet) > 0) {
      startConsoleReporter();
      startMemoryReporter();
      startRecordReporter();
      if (monitorWrites) {
        if (batchingEnabled) {
          startBatchesReporter();
        }
        startWritesReporter();
      } else {
        startReadsReporter();
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
            .inDomain(DSBULK_JMX_DOMAIN)
            .createsObjectNamesWith(
                (metricsType, jmxDomain, metricName) -> {
                  try {
                    StringBuilder sb =
                        new StringBuilder(jmxDomain)
                            .append(":executionId=")
                            .append(JMXUtils.quoteJMXIfNecessary(executionId))
                            .append(',');
                    StringTokenizer tokenizer = new StringTokenizer(metricName, "/");
                    int i = 1;
                    while (tokenizer.hasMoreTokens()) {
                      String token = tokenizer.nextToken();
                      if (tokenizer.hasMoreTokens()) {
                        sb.append("level").append(i++);
                      } else {
                        sb.append("name");
                      }
                      sb.append('=').append(JMXUtils.quoteJMXIfNecessary(token));
                      if (tokenizer.hasMoreTokens()) {
                        sb.append(',');
                      }
                    }
                    return ObjectName.getInstance(sb.toString());
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
            .build(operationDirectory.toFile());
    csvReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  private void startRecordReporter() {
    recordReporter = new RecordReporter(registry, logSink, rateUnit, scheduler, expectedWrites);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(Verbosity.normal) > 0) {
      recordReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startBatchesReporter() {
    batchesReporter = new BatchReporter(registry, logSink, scheduler);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(Verbosity.normal) > 0) {
      batchesReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startMemoryReporter() {
    memoryReporter = new MemoryReporter(registry, logSink, scheduler);
    // periodic reporting is only enabled in verbose mode
    if (verbosity.compareTo(Verbosity.normal) > 0) {
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
    if (verbosity.compareTo(Verbosity.normal) > 0) {
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
    if (verbosity.compareTo(Verbosity.normal) > 0) {
      readsReporter.start(reportInterval.getSeconds(), SECONDS);
    }
  }

  private void startConsoleReporter() {
    if (monitorWrites) {
      registry.counter("records/failed");
      consoleReporter =
          new ConsoleReporter(
              registry,
              running,
              () -> totalItems.getCount(),
              () -> failedItems.getCount(),
              listener.getTotalWritesTimer(),
              listener.getBytesSentMeter().orElse(null),
              batchingEnabled ? registry.histogram("batches/size") : null,
              SECONDS,
              MILLISECONDS,
              expectedWrites,
              scheduler,
              rowType);
    } else {
      consoleReporter =
          new ConsoleReporter(
              registry,
              running,
              () -> totalItems.getCount(),
              () -> failedItems.getCount(),
              listener.getTotalReadsTimer(),
              listener.getBytesReceivedMeter().orElse(null),
              null,
              SECONDS,
              MILLISECONDS,
              expectedReads,
              scheduler,
              rowType);
    }
    consoleReporter.start(reportInterval.getSeconds(), SECONDS);
  }

  public void stop() {
    if (consoleReporter != null) {
      // print one last report to get final numbers on the console,
      // if the workflow hasn't been interrupted,
      // and before any closing message is printed.
      consoleReporter.report();
      consoleReporter = null;
    }
    running.set(false);
  }

  @Override
  public void close() {
    stop();
    if (consoleReporter != null) {
      consoleReporter.close();
    }
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
    MoreExecutors.shutdownAndAwaitTermination(scheduler, 1, MINUTES);
  }

  public void reportFinalMetrics() {
    if (recordReporter != null
        || batchesReporter != null
        || memoryReporter != null
        || writesReporter != null
        || readsReporter != null) {
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
  }

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsMonitor() {
    return upstream -> upstream.doOnNext(item -> totalItems.inc());
  }

  public <T> Function<Flux<T>, Flux<T>> newFailedItemsMonitor() {
    return upstream ->
        upstream.doOnNext(
            item -> {
              if (item instanceof ErrorRecord
                  || item instanceof UnmappableStatement
                  || (item instanceof Result && !((Result) item).isSuccess())) {
                failedItems.inc();
              }
            });
  }

  public Function<Flux<Statement<?>>, Flux<Statement<?>>> newBatcherMonitor() {
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
