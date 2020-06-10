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
package com.datastax.oss.dsbulk.workflow.count;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.commons.utils.DurationUtils;
import com.datastax.oss.dsbulk.commons.utils.ThrowableUtils;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.sampler.DataSizeSampler;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManager;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.settings.CodecSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.DriverSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.EngineSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ExecutorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.MonitoringSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaGenerationType;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SettingsManager;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode;
import com.datastax.oss.dsbulk.workflow.commons.utils.CloseableUtils;
import com.datastax.oss.dsbulk.workflow.commons.utils.ClusterInformationUtils;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/** The main class for count workflows. */
public class CountWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(CountWorkflow.class);

  private static final int _1KB = 1024;
  private static final int _10_KB = 10 * _1KB;

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Scheduler scheduler;
  private ReadResultCounter readResultCounter;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private CqlSession session;
  private BulkReader executor;
  private List<? extends Statement<?>> readStatements;
  private volatile boolean success;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<ReadResult>, Flux<ReadResult>> queryWarningsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private int readConcurrency;
  private int numCores;
  private boolean useThreadPerCore;
  private SchemaSettings schemaSettings;
  private ExecutorSettings executorSettings;

  CountWorkflow(Config config) {
    settingsManager = new SettingsManager(config);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init("COUNT", false);
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    StatsSettings statsSettings = settingsManager.getStatsSettings();
    engineSettings.init();
    // First verify that dry-run is off; that's unsupported for count.
    if (engineSettings.isDryRun()) {
      throw new IllegalArgumentException("Dry-run is not supported for count");
    }
    logSettings.init();
    driverSettings.init(false);
    logSettings.logEffectiveSettings(
        settingsManager.getEffectiveBulkLoaderConfig(), driverSettings.getDriverConfig());
    codecSettings.init();
    monitoringSettings.init();
    executorSettings.init();
    statsSettings.init();
    session = driverSettings.newSession(executionId);
    ClusterInformationUtils.printDebugInfoAboutCluster(session);
    schemaSettings.init(SchemaGenerationType.READ_AND_COUNT, session, false, false);
    logManager = logSettings.newLogManager(session, false);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            false,
            false,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getMetrics().map(Metrics::getRegistry).orElse(new MetricRegistry()),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry(),
            schemaSettings.getRowType());
    metricsManager.init();
    executor =
        executorSettings.newReadExecutor(session, metricsManager.getExecutionListener(), false);
    ConvertingCodecFactory codecFactory =
        codecSettings.createCodecFactory(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    EnumSet<StatsSettings.StatisticsMode> modes = statsSettings.getStatisticsModes();
    int numPartitions = statsSettings.getNumPartitions();
    readResultCounter =
        schemaSettings.createReadResultCounter(session, codecFactory, modes, numPartitions);
    readStatements = schemaSettings.createReadStatements(session);
    closed.set(false);
    success = false;
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedItemsMonitor = metricsManager.newFailedItemsMonitor();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedReadsHandler = logManager.newFailedReadsHandler();
    queryWarningsHandler = logManager.newQueryWarningsHandler();
    terminationHandler = logManager.newTerminationHandler();
    numCores = Runtime.getRuntime().availableProcessors();
    scheduler = Schedulers.newParallel(numCores, new DefaultThreadFactory("workflow"));
    useThreadPerCore =
        readStatements.size() >= Math.max(4, Runtime.getRuntime().availableProcessors() / 4)
            // parallel flux is not compatible with stats modes other than global
            || !statsSettings.getStatisticsModes().stream()
                .allMatch(mode -> mode == StatisticsMode.global);
    LOGGER.debug("Using thread-per-core strategy: {}", useThreadPerCore);
    readConcurrency =
        engineSettings.getMaxConcurrentQueries().orElseGet(this::determineReadConcurrency);
    LOGGER.debug(
        "Using read concurrency: {} (user-supplied: {})",
        readConcurrency,
        engineSettings.getMaxConcurrentQueries().isPresent());
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    (useThreadPerCore ? threadPerCoreFlux() : parallelFlux())
        .transform(terminationHandler)
        .blockLast();
    timer.stop();
    metricsManager.stop();
    Duration elapsed = DurationUtils.round(timer.elapsed(), TimeUnit.SECONDS);
    if (logManager.getTotalErrors() == 0) {
      success = true;
      LOGGER.info("{} completed successfully in {}.", this, DurationUtils.formatDuration(elapsed));
    } else {
      LOGGER.warn(
          "{} completed with {} errors in {}.",
          this,
          logManager.getTotalErrors(),
          DurationUtils.formatDuration(elapsed));
    }
    return logManager.getTotalErrors() == 0;
  }

  @NonNull
  private Flux<Void> threadPerCoreFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                Flux.from(executor.readReactive(statement))
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedItemsMonitor)
                    .transform(failedReadsHandler)
                    // Important:
                    // 1) there must be one counting unit per thread / inner flow:
                    // this is guaranteed by instantiating a new counting unit below for each
                    // inner flow.
                    // 2) When counting partitions or ranges, a partition cannot be split in two
                    // inner flows; this is guaranteed in the thread-per-core strategy since
                    // statements are split by token range (users cannot supply a custom query for
                    // these counting modes);
                    .doOnNext(readResultCounter.newCountingUnit()::update)
                    .then()
                    .subscribeOn(scheduler),
            readConcurrency);
  }

  @NonNull
  private Flux<Void> parallelFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(executor::readReactive, readConcurrency)
        .window(Queues.SMALL_BUFFER_SIZE)
        .flatMap(
            results ->
                results
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedItemsMonitor)
                    .transform(failedReadsHandler)
                    // Parallel flux does not preserve one-to-one mapping between token ranges
                    // and counting units; it is only suitable for the global counting mode.
                    .doOnNext(readResultCounter.newCountingUnit()::update)
                    .then()
                    .subscribeOn(scheduler),
            numCores);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = CloseableUtils.closeQuietly(readResultCounter, null);
      e = CloseableUtils.closeQuietly(metricsManager, e);
      e = CloseableUtils.closeQuietly(logManager, e);
      e = CloseableUtils.closeQuietly(scheduler, e);
      e = CloseableUtils.closeQuietly(executor, e);
      e = CloseableUtils.closeQuietly(session, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      // only print totals if the operation was successful
      if (success) {
        readResultCounter.reportTotals();
      }
      LOGGER.debug("{} closed.", this);
      if (e != null) {
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    if (executionId == null) {
      return "Operation";
    } else {
      return "Operation " + executionId;
    }
  }

  private int determineReadConcurrency() {
    LOGGER.debug("Sampling data...");
    RelationMetadata table = schemaSettings.getTargetTable();
    // Read the entire row to get a more accurate picture of the expected throughput
    String fullReadCql =
        String.format(
            "SELECT * FROM %s.%s", table.getKeyspace().asCql(true), table.getName().asCql(true));
    SimpleStatement fullReadStatement =
        SimpleStatement.newInstance(fullReadCql)
            .setPageSize(1000)
            .setTimeout(Duration.ofMinutes(1));
    Histogram sample =
        DataSizeSampler.sampleReads(
            Flux.just(fullReadStatement)
                .<Row>flatMap(session::executeReactive)
                .onErrorContinue(
                    (error, v) ->
                        LOGGER.debug(
                            "Sampling failed: {}", ThrowableUtils.getSanitizedErrorMessage(error)))
                .take(1000)
                .toIterable());
    double meanSize;
    if (sample.getCount() < 100) {
      // sample too small, go with a common value
      LOGGER.debug("Data sample is too small: {}, discarding", sample.getCount());
      meanSize = _1KB;
    } else {
      meanSize = sample.getSnapshot().getMean();
      LOGGER.debug("Average row size in bytes: {}", meanSize);
    }
    int readConcurrency;
    if (meanSize < 128) {
      readConcurrency = numCores * 16;
    } else if (meanSize < 512) {
      readConcurrency = numCores * 8;
    } else if (meanSize < _1KB) {
      readConcurrency = numCores * 4;
    } else if (meanSize < _10_KB) {
      readConcurrency = numCores * 2;
    } else {
      readConcurrency = numCores;
    }
    if (executorSettings.isContinuousPagingEnabled()) {
      // When using CP we cannot go over the maximum number of CP sessions per node,
      // which by default is 60. Since this is a per-node limit, but we never hit
      // just one node, we consider twice as many queries per node.
      int maxContinuousPagingSessionsPerCluster =
          (int)
              (session.getMetadata().getNodes().values().stream()
                      .filter(node -> node.getState() == NodeState.UP)
                      .count()
                  * 60
                  * 2);
      readConcurrency = Math.min(readConcurrency, maxContinuousPagingSessionsPerCluster);
    }
    return readConcurrency;
  }
}
