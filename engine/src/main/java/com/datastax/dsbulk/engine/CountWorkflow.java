/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.TPC_THRESHOLD;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
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
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.google.common.base.Stopwatch;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
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
  private List<Statement> readStatements;

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
    engineSettings.init();
    // First verify that dry-run is off; that's unsupported for count.
    if (engineSettings.isDryRun()) {
      throw new BulkConfigurationException("Dry-run is not supported for count");
    }
    logSettings.init(false);
    logSettings.logEffectiveSettings(settingsManager.getGlobalConfig());
    codecSettings.init();
    schemaSettings.init(WorkflowType.COUNT);
    monitoringSettings.init();
    executorSettings.init();
    driverSettings.init();
    scheduler =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    cluster = driverSettings.newCluster();
    DseSession session = cluster.connect();
    logManager = logSettings.newLogManager(WorkflowType.COUNT, cluster);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.COUNT,
            false,
            logManager.getExecutionDirectory(),
            logSettings.getMainLogFileAppender(),
            cluster.getMetrics().getRegistry(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
    metricsManager.init();
    executor = executorSettings.newReadExecutor(session, metricsManager.getExecutionListener());
    readResultCounter = schemaSettings.createReadResultCounter(session);
    readStatements = schemaSettings.createReadStatements(cluster);
    closed.set(false);
  }

  @Override
  public boolean execute() {
    LOGGER.info("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Void> flux;
    if (readStatements.size() >= TPC_THRESHOLD) {
      flux = threadPerCoreFlux();
    } else {
      flux = parallelFlux();
    }
    flux.transform(logManager.newTerminationHandler()).blockLast();
    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    if (logManager.getTotalErrors() == 0) {
      LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
    } else {
      LOGGER.info(
          "{} completed with {} errors in {}.",
          this,
          logManager.getTotalErrors(),
          WorkflowUtils.formatElapsed(seconds));
    }
    return logManager.getTotalErrors() == 0;
  }

  @NotNull
  private Flux<Void> threadPerCoreFlux() {
    LOGGER.info("Using thread-per-core pattern.");
    return Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                executor
                    .readReactive(statement)
                    .transform(metricsManager.newTotalItemsMonitor())
                    .transform(logManager.newTotalItemsCounter())
                    .transform(metricsManager.newFailedItemsMonitor())
                    .transform(logManager.newFailedReadsHandler())
                    .doOnNext(readResultCounter::update)
                    .then()
                    .subscribeOn(scheduler),
            Runtime.getRuntime().availableProcessors());
  }

  @NotNull
  private Flux<Void> parallelFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(executor::readReactive)
        .parallel()
        .runOn(scheduler)
        .composeGroup(metricsManager.newTotalItemsMonitor())
        .composeGroup(logManager.newTotalItemsCounter())
        .composeGroup(metricsManager.newFailedItemsMonitor())
        .composeGroup(logManager.newFailedReadsHandler())
        .doOnNext(readResultCounter::update)
        .sequential()
        .then()
        .flux();
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.info("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(scheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(cluster, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      if (readResultCounter != null) {
        readResultCounter.reportTotals();
      }
      LOGGER.info("{} closed.", this);
      if (e != null) {
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    return "Count workflow engine execution " + executionId;
  }
}
