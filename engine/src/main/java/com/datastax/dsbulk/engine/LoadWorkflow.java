/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.engine.internal.WorkflowUtils.checkProductCompatibility;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.engine.internal.WorkflowUtils;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.schema.RecordMapper;
import com.datastax.dsbulk.engine.internal.settings.BatchSettings;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.settings.ConnectorSettings;
import com.datastax.dsbulk.engine.internal.settings.DriverSettings;
import com.datastax.dsbulk.engine.internal.settings.EngineSettings;
import com.datastax.dsbulk.engine.internal.settings.ExecutorSettings;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.settings.MonitoringSettings;
import com.datastax.dsbulk.engine.internal.settings.SchemaSettings;
import com.datastax.dsbulk.engine.internal.settings.SettingsManager;
import com.datastax.dsbulk.executor.api.batch.ReactorStatementBatcher;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.writer.ReactorBulkWriter;
import com.google.common.base.Stopwatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/** The main class for load workflows. */
public class LoadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadWorkflow.class);

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.LOAD);
  private final LoaderConfig config;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private Connector connector;
  private Scheduler scheduler;
  private RecordMapper recordMapper;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private ReactorStatementBatcher batcher;
  private DseCluster cluster;
  private ReactorBulkWriter executor;
  private boolean batchingEnabled;
  private boolean dryRun;
  private int resourceCount;
  private int batchBufferSize;
  private int maxInFlight;

  LoadWorkflow(LoaderConfig config) {
    this.config = config;
  }

  @Override
  public void init() throws Exception {
    SettingsManager settingsManager = new SettingsManager(config, executionId, WorkflowType.LOAD);
    settingsManager.loadConfiguration();
    settingsManager.logEffectiveSettings();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    BatchSettings batchSettings = settingsManager.getBatchSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    maxInFlight = executorSettings.getMaxInFlight();
    dryRun = engineSettings.isDryRun();
    scheduler = Schedulers.newElastic("workflow");
    connector = connectorSettings.getConnector();
    connector.init();
    cluster = driverSettings.newCluster();
    checkProductCompatibility(cluster);
    String keyspace = schemaSettings.getKeyspace();
    DseSession session = cluster.connect(keyspace);
    batchingEnabled = batchSettings.isBatchingEnabled();
    batchBufferSize = batchSettings.getBufferSize();
    metricsManager = monitoringSettings.newMetricsManager(WorkflowType.LOAD, batchingEnabled);
    metricsManager.init();
    logManager = logSettings.newLogManager(WorkflowType.LOAD, cluster, scheduler);
    logManager.init();
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    recordMapper =
        schemaSettings.createRecordMapper(
            session, connector.getRecordMetadata(), codecSettings.createCodecRegistry(cluster));
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(cluster);
    }
    closed.set(false);
    resourceCount = connector.estimatedResourceCount();
  }

  @Override
  public void execute() throws InterruptedException {
    LOGGER.info("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Void> flux;
    if (resourceCount >= WorkflowUtils.TPC_THRESHOLD) {
      flux = threadPerCoreFlux();
    } else if (batchingEnabled) {
      flux = parallelBatchedFlux();
    } else {
      flux = parallelUnbatchedFlux();
    }
    flux.blockLast();
    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
  }

  @NotNull
  private Flux<Void> threadPerCoreFlux() {
    LOGGER.info("Using thread-per-core pattern.");
    return Flux.from(connector.readByResource())
        .flatMap(
            records -> {
              Flux<Statement> stmts =
                  Flux.from(records)
                      .transform(metricsManager.newUnmappableRecordMonitor())
                      .transform(logManager.newUnmappableRecordErrorHandler())
                      .map(recordMapper::map)
                      .transform(metricsManager.newUnmappableStatementMonitor())
                      .transform(logManager.newUnmappableStatementErrorHandler());
              if (batchingEnabled) {
                stmts =
                    stmts
                        .window(batchBufferSize)
                        .flatMap(batcher::batchByGroupingKey)
                        .transform(metricsManager.newBatcherMonitor());
              }
              return executeStatements(stmts).subscribeOn(scheduler);
            },
            Runtime.getRuntime().availableProcessors());
  }

  @NotNull
  private Flux<Void> parallelBatchedFlux() {
    return Flux.from(connector.readByResource())
        .flatMap(records -> Flux.from(records).window(batchBufferSize))
        .publishOn(scheduler, Queues.SMALL_BUFFER_SIZE * 4)
        .parallel()
        .runOn(scheduler)
        .flatMap(
            records ->
                records
                    .transform(metricsManager.newUnmappableRecordMonitor())
                    .transform(logManager.newUnmappableRecordErrorHandler())
                    .map(recordMapper::map)
                    .transform(metricsManager.newUnmappableStatementMonitor())
                    .transform(logManager.newUnmappableStatementErrorHandler())
                    .transform(batcher::batchByGroupingKey)
                    .transform(metricsManager.newBatcherMonitor())
                    .transform(this::executeStatements))
        .sequential();
  }

  @NotNull
  private Flux<Void> parallelUnbatchedFlux() {
    return Flux.from(connector.read())
        .publishOn(scheduler, Queues.SMALL_BUFFER_SIZE * 4)
        .parallel()
        .runOn(scheduler)
        .composeGroup(metricsManager.newUnmappableRecordMonitor())
        .composeGroup(logManager.newUnmappableRecordErrorHandler())
        .map(recordMapper::map)
        .composeGroup(metricsManager.newUnmappableStatementMonitor())
        .composeGroup(logManager.newUnmappableStatementErrorHandler())
        .composeGroup(this::executeStatements)
        .sequential();
  }

  @NotNull
  private Flux<Void> executeStatements(Flux<Statement> stmts) {
    Flux<WriteResult> results;
    if (dryRun) {
      results = stmts.map(s -> new DefaultWriteResult(s, null));
    } else {
      results = stmts.flatMap(executor::writeReactive, maxInFlight);
    }
    return results
        .transform(logManager.newWriteErrorHandler())
        .transform(logManager.newResultPositionTracker());
  }

  @SuppressWarnings("Duplicates")
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.info("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(connector, e);
      e = WorkflowUtils.closeQuietly(scheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(cluster, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      if (logManager != null) {
        logManager.reportLastLocations();
      }
      LOGGER.info("{} closed.", this);
      if (e != null) {
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    return "Load workflow engine execution " + executionId;
  }
}
