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

import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
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
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import io.netty.util.concurrent.DefaultThreadFactory;
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
  private DseSession session;
  private ReactorBulkReader executor;
  private List<? extends Statement<?>> readStatements;
  private volatile boolean success;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedItemsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;

  CountWorkflow(LoaderConfig config, BiMap<String, String> shortcuts) {
    settingsManager = new SettingsManager(config, shortcuts, WorkflowType.COUNT);
  }

  @Override
  public void init() throws Exception {
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
    driverSettings.init(false);
    logSettings.logEffectiveSettings(
        settingsManager.getBulkLoaderConfig(), driverSettings.getDriverConfig());
    codecSettings.init();
    monitoringSettings.init();
    executorSettings.init();
    statsSettings.init();
    scheduler =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    session = driverSettings.newSession(executionId);
    printDebugInfoAboutCluster(session);
    schemaSettings.init(WorkflowType.COUNT, session, true, true);
    logManager = logSettings.newLogManager(WorkflowType.COUNT, session);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.COUNT,
            false,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getMetrics().map(Metrics::getRegistry).orElse(new MetricRegistry()),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry());
    metricsManager.init();
    executor =
        executorSettings.newReadExecutor(session, metricsManager.getExecutionListener(), false);
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    EnumSet<StatsSettings.StatisticsMode> modes = statsSettings.getStatisticsModes();
    int numPartitions = statsSettings.getNumPartitions();
    readResultCounter =
        schemaSettings.createReadResultCounter(session, codecRegistry, modes, numPartitions);
    readStatements = schemaSettings.createReadStatements(session);
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
      LOGGER.info("{} completed successfully in {}.", this, StringUtils.formatElapsed(seconds));
    } else {
      LOGGER.warn(
          "{} completed with {} errors in {}.",
          this,
          logManager.getTotalErrors(),
          StringUtils.formatElapsed(seconds));
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
      e = WorkflowUtils.closeQuietly(session, e);
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
