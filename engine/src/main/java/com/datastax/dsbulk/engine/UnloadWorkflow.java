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
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.commons.config.CodecSettings;
import com.datastax.dsbulk.engine.internal.settings.ConnectorSettings;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for unload workflows. */
public class UnloadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
  private Scheduler scheduler;
  private ReadResultMapper readResultMapper;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private DseCluster cluster;
  private ReactorBulkReader executor;
  private List<Statement> readStatements;

  UnloadWorkflow(LoaderConfig config) {
    settingsManager = new SettingsManager(config, WorkflowType.UNLOAD);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init();
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    engineSettings.init();
    // First verify that dry-run is off; that's unsupported for unload.
    if (engineSettings.isDryRun()) {
      throw new BulkConfigurationException("Dry-run is not supported for unload");
    }
    connectorSettings.init();
    connector = connectorSettings.getConnector();
    connector.init();
    // No logs should be produced until the following statement returns
    logSettings.init();
    logSettings.logEffectiveSettings(settingsManager.getGlobalConfig());
    codecSettings.init();
    schemaSettings.init(
        WorkflowType.UNLOAD, !connector.supports(CommonConnectorFeature.MAPPED_RECORDS));
    monitoringSettings.init();
    executorSettings.init();
    driverSettings.init();
    scheduler =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    cluster = driverSettings.newCluster();
    DseSession session = cluster.connect();
    logManager = logSettings.newLogManager(WorkflowType.UNLOAD, cluster);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.UNLOAD,
            false,
            logManager.getExecutionDirectory(),
            logSettings.getVerbosity(),
            cluster.getMetrics().getRegistry(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
    metricsManager.init();
    executor = executorSettings.newReadExecutor(session, metricsManager.getExecutionListener());
    RecordMetadata recordMetadata = connector.getRecordMetadata();
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(cluster.getConfiguration().getCodecRegistry());
    readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
    readStatements = schemaSettings.createReadStatements(cluster);
    closed.set(false);
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Record> flux;
    if (readStatements.size() >= TPC_THRESHOLD) {
      flux = threadPerCoreFlux();
    } else {
      flux = parallelFlux();
    }
    flux.compose(connector.write())
        .transform(metricsManager.newFailedItemsMonitor())
        .transform(logManager.newFailedRecordsHandler())
        .then()
        .flux()
        .transform(logManager.newTerminationHandler())
        .blockLast();
    timer.stop();
    metricsManager.stop();
    long seconds = timer.elapsed(SECONDS);
    if (logManager.getTotalErrors() == 0) {
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

  @NotNull
  private Flux<Record> threadPerCoreFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                executor
                    .readReactive(statement)
                    .transform(metricsManager.newTotalItemsMonitor())
                    .transform(logManager.newTotalItemsCounter())
                    .transform(metricsManager.newFailedItemsMonitor())
                    .transform(logManager.newFailedReadsHandler())
                    .map(readResultMapper::map)
                    .transform(metricsManager.newFailedItemsMonitor())
                    .transform(logManager.newUnmappableRecordsHandler())
                    .subscribeOn(scheduler),
            Runtime.getRuntime().availableProcessors());
  }

  @NotNull
  private Flux<Record> parallelFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(executor::readReactive)
        .transform(metricsManager.newTotalItemsMonitor())
        .transform(logManager.newTotalItemsCounter())
        .transform(metricsManager.newFailedItemsMonitor())
        .transform(logManager.newFailedReadsHandler())
        .parallel()
        .runOn(scheduler)
        .map(readResultMapper::map)
        .sequential()
        .transform(metricsManager.newFailedItemsMonitor())
        .transform(logManager.newUnmappableRecordsHandler());
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(connector, e);
      e = WorkflowUtils.closeQuietly(scheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(cluster, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      LOGGER.debug("{} closed.", this);
      if (e != null) {
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    return "Operation " + executionId;
  }
}
