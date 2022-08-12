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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.utils.DurationUtils;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManager;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultCounter;
import com.datastax.oss.dsbulk.workflow.commons.settings.CodecSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.DriverSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.EngineSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ExecutorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.MonitoringSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaGenerationStrategy;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SettingsManager;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.utils.CloseableUtils;
import com.datastax.oss.dsbulk.workflow.commons.utils.ClusterInformationUtils;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for count workflows. */
public class CountWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(CountWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Scheduler scheduler;
  private ReadResultCounter readResultCounter;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private CqlSession session;
  private BulkReader executor;
  private List<RangeReadBoundStatement> readStatements;
  private volatile boolean success;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<ReadResult>, Flux<ReadResult>> queryWarningsHandler;
  private BiFunction<URI, Publisher<ReadResult>, Publisher<ReadResult>> resourceStatsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private Function<Flux<ReadResult>, Flux<Void>> resultPositionsHandler;
  private int readConcurrency;

  CountWorkflow(Config config) {
    settingsManager = new SettingsManager(config);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init("COUNT", false, SchemaGenerationStrategy.READ_AND_COUNT);
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
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
    ConvertingCodecFactory codecFactory =
        codecSettings.createCodecFactory(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    session =
        driverSettings.newSession(
            executionId, codecFactory.getCodecRegistry(), monitoringSettings.getRegistry());
    ClusterInformationUtils.printDebugInfoAboutCluster(session);
    schemaSettings.init(session, codecFactory, false, false);
    logManager = logSettings.newLogManager(session);
    logManager.init();
    if (executorSettings.isTrackingBytes()) {
      monitoringSettings.forceTrackBytes();
    }
    metricsManager =
        monitoringSettings.newMetricsManager(
            false,
            false,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry(),
            schemaSettings.getRowType());
    metricsManager.init();
    executor =
        executorSettings.newReadExecutor(session, metricsManager.getExecutionListener(), false);
    EnumSet<StatsSettings.StatisticsMode> modes = statsSettings.getStatisticsModes();
    int numPartitions = statsSettings.getNumPartitions();
    readResultCounter =
        schemaSettings.createReadResultCounter(session, codecFactory, modes, numPartitions);
    readStatements = schemaSettings.createReadStatements(session);
    closed.set(false);
    success = false;
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedItemsMonitor = metricsManager.newFailedResultsMonitor();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedReadsHandler = logManager.newFailedReadsHandler();
    queryWarningsHandler = logManager.newQueryWarningsHandler();
    resourceStatsHandler = logManager.newCqlResourceStatsHandler();
    resultPositionsHandler = logManager.newReadResultPositionsHandler();
    terminationHandler = logManager.newTerminationHandler();
    int numCores = Runtime.getRuntime().availableProcessors();
    readConcurrency =
        Math.min(readStatements.size(), engineSettings.getMaxConcurrentQueries().orElse(numCores));
    LOGGER.debug(
        "Using read concurrency: {} (user-supplied: {})",
        readConcurrency,
        engineSettings.getMaxConcurrentQueries().isPresent());
    int numThreads = Math.min(readConcurrency, numCores);
    scheduler = Schedulers.newParallel(numThreads, new DefaultThreadFactory("workflow"));
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                Flux.from(executor.readReactive(statement))
                    .transform(
                        upstream -> resourceStatsHandler.apply(statement.getResource(), upstream))
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedItemsMonitor)
                    .transform(failedReadsHandler)
                    // Important:
                    // 1) there must be one counting unit per inner flow: this is guaranteed by
                    // instantiating a new counting unit below for each inner flow.
                    // 2) When counting partitions or ranges, a partition cannot be split in two
                    // inner flows; this is guaranteed since statements are split by token range
                    // (users cannot supply a custom query for these counting modes).
                    .doOnNext(readResultCounter.newCountingUnit()::update)
                    .subscribeOn(scheduler),
            readConcurrency)
        .transform(resultPositionsHandler)
        .transform(terminationHandler)
        .blockLast();
    timer.stop();
    int totalErrors = logManager.getTotalErrors();
    metricsManager.stop(timer.elapsed(), totalErrors == 0);
    Duration elapsed = DurationUtils.round(timer.elapsed(), TimeUnit.SECONDS);
    String elapsedStr =
        elapsed.isZero() ? "less than one second" : DurationUtils.formatDuration(elapsed);
    if (totalErrors == 0) {
      success = true;
      LOGGER.info("{} completed successfully in {}.", this, elapsedStr);
    } else {
      LOGGER.warn("{} completed with {} errors in {}.", this, totalErrors, elapsedStr);
    }
    return totalErrors == 0;
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
      if (logManager != null) {
        logManager.reportAvailableFiles();
      }

      // Print results even if any failures.
      // It has no sense to query billions rows for a few hours and show nothing if some failure
      // happens
      if (readResultCounter != null) {
        readResultCounter.reportTotals();
        if (!success) {
          LOGGER.warn(
              "Please note: the totals reported above are probably inaccurate, "
                  + "since the operation completed with errors.");
        }
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
}
