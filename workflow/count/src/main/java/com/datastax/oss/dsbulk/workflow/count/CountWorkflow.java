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

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.commons.utils.DurationUtils;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
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
import com.datastax.oss.dsbulk.workflow.commons.utils.CloseableUtils;
import com.datastax.oss.dsbulk.workflow.commons.utils.ClusterInformationUtils;
import com.typesafe.config.Config;
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
  private List<? extends Statement<?>> readStatements;
  private volatile boolean success;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private int readConcurrency;

  CountWorkflow(Config config) {
    settingsManager = new SettingsManager(config);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init("COUNT", false);
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
    terminationHandler = logManager.newTerminationHandler();
    int numCores = Runtime.getRuntime().availableProcessors();
    scheduler = Schedulers.newParallel(numCores, new DefaultThreadFactory("workflow"));
    readConcurrency = engineSettings.getMaxConcurrentQueries().orElse(numCores * 8);
    LOGGER.debug("Using read concurrency: " + readConcurrency);
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
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedItemsMonitor)
                    .transform(failedReadsHandler)
                    // Important:
                    // 1) there must be one counting unit per thread / inner flow:
                    // this is guaranteed by instantiating a new counting unit below for each
                    // inner flow.
                    // 2) A partition cannot be split in two inner flows;
                    // this is guaranteed by the way we create our read statements by token range.
                    .doOnNext(readResultCounter.newCountingUnit()::update)
                    .then()
                    .subscribeOn(scheduler),
            readConcurrency)
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
}
