/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.printDebugInfoAboutCluster;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.schema.ReadResultCounter;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.settings.DriverSettings;
import com.datastax.dsbulk.engine.internal.settings.EngineSettings;
import com.datastax.dsbulk.engine.internal.settings.ExecutorSettings;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.settings.MonitoringSettings;
import com.datastax.dsbulk.engine.internal.settings.SchemaSettings;
import com.datastax.dsbulk.engine.internal.settings.SettingsManager;
import com.datastax.dsbulk.engine.internal.settings.StatsSettings;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.google.common.base.Stopwatch;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for count workflows. */
public class CountWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Scheduler scheduler;
  private ReadResultCounter readResultCounter;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private DseCluster cluster;
  private ReactorBulkReader executor;
  private List<? extends Statement> readStatements;
  private volatile boolean success;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;

  CountWorkflow(LoaderConfig config) {
    settingsManager = new SettingsManager(config, WorkflowType.COUNT);
  }

  @Override
  public void init() throws IOException {
    settingsManager.init();
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
      throw new BulkConfigurationException("Dry-run is not supported for count");
    }
    logSettings.init();
    logSettings.logEffectiveSettings(settingsManager.getGlobalConfig());
    codecSettings.init();
    monitoringSettings.init();
    executorSettings.init();
    driverSettings.init();
    statsSettings.init();
    scheduler =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    cluster = driverSettings.newCluster();
    cluster.init();
    driverSettings.checkProtocolVersion(cluster);
    printDebugInfoAboutCluster(cluster);
    schemaSettings.init(WorkflowType.COUNT, cluster, true, true);
    DseSession session = cluster.connect();
    logManager = logSettings.newLogManager(WorkflowType.COUNT, cluster);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.COUNT,
            false,
            logManager.getExecutionDirectory(),
            logSettings.getVerbosity(),
            cluster.getMetrics().getRegistry(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry(),
            schemaSettings.getRowType());
    metricsManager.init();
    executor =
        executorSettings.newReadExecutor(session, metricsManager.getExecutionListener(), false);
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(
            cluster.getConfiguration().getCodecRegistry(),
            schemaSettings.isAllowExtraFields(),
            schemaSettings.isAllowMissingFields());
    EnumSet<StatsSettings.StatisticsMode> modes = statsSettings.getStatisticsModes();
    int numPartitions = statsSettings.getNumPartitions();
    readResultCounter =
        schemaSettings.createReadResultCounter(session, codecRegistry, modes, numPartitions);
    readStatements = schemaSettings.createReadStatements(cluster);
    closed.set(false);
    success = false;
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedItemsMonitor = metricsManager.newFailedItemsMonitor();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedReadsHandler = logManager.newFailedReadsHandler();
    terminationHandler = logManager.newTerminationHandler();
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                executor
                    .readReactive(statement)
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
            Runtime.getRuntime().availableProcessors())
        .transform(terminationHandler)
        .blockLast();
    timer.stop();
    metricsManager.stop();
    long seconds = timer.elapsed(SECONDS);
    if (logManager.getTotalErrors() == 0) {
      success = true;
      LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
    } else {
      LOGGER.warn(
          "{} completed with {} errors in {}.",
          this,
          logManager.getTotalErrors(),
          WorkflowUtils.formatElapsed(seconds));
    }
    return logManager.getTotalErrors() == 0;
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(readResultCounter, null);
      e = WorkflowUtils.closeQuietly(metricsManager, e);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(scheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(cluster, e);
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
