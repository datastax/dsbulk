/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.checkProductCompatibility;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.Connector;
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
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.reactor.batch.ReactorStatementBatcher;
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;
import com.google.common.base.Stopwatch;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/** The main class for load workflows. */
public class LoadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
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
  private Set<Disposable> disposables;

  LoadWorkflow(LoaderConfig config) {
    settingsManager = new SettingsManager(config, WorkflowType.LOAD);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init();
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    logSettings.init();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    connectorSettings.init();
    connector = connectorSettings.getConnector();
    connector.init();
    logSettings.logEffectiveSettings(settingsManager.getGlobalConfig());
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    BatchSettings batchSettings = settingsManager.getBatchSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    monitoringSettings.init();
    codecSettings.init();
    batchSettings.init();
    driverSettings.init();
    executorSettings.init();
    engineSettings.init();
    schemaSettings.init(WorkflowType.LOAD);
    cluster = driverSettings.newCluster();
    checkProductCompatibility(cluster);
    DseSession session = cluster.connect();
    batchingEnabled = batchSettings.isBatchingEnabled();
    batchBufferSize = batchSettings.getBufferSize();
    logManager = logSettings.newLogManager(WorkflowType.LOAD, cluster);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.LOAD,
            batchingEnabled,
            logManager.getExecutionDirectory(),
            logSettings.getVerbosity(),
            cluster.getMetrics().getRegistry(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
    metricsManager.init();
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    recordMapper =
        schemaSettings.createRecordMapper(
            session,
            connector.getRecordMetadata(),
            codecSettings.createCodecRegistry(cluster.getConfiguration().getCodecRegistry()),
            !connector.supports(CommonConnectorFeature.MAPPED_RECORDS));
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(cluster);
    }
    maxInFlight = executorSettings.getMaxInFlight();
    dryRun = engineSettings.isDryRun();
    if (dryRun) {
      LOGGER.info("Dry-run mode enabled.");
    }
    closed.set(false);
    resourceCount = connector.estimatedResourceCount();
    disposables = new HashSet<>();
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Void> flux;
    if (resourceCount >= WorkflowUtils.TPC_THRESHOLD) {
      flux = threadPerCoreFlux();
    } else {
      flux = parallelFlux();
    }
    flux.transform(logManager.newTerminationHandler()).blockLast();
    timer.stop();
    metricsManager.stopProgress();
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
  private Flux<Void> threadPerCoreFlux() {
    Scheduler scheduler =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    disposables.add(scheduler);
    int concurrency =
        Math.max(Queues.XS_BUFFER_SIZE, maxInFlight / Runtime.getRuntime().availableProcessors());
    return Flux.defer(connector.readByResource())
        .flatMap(
            records -> {
              Flux<Statement> stmts =
                  Flux.from(records)
                      .transform(metricsManager.newTotalItemsMonitor())
                      .transform(logManager.newTotalItemsCounter())
                      .transform(metricsManager.newFailedItemsMonitor())
                      .transform(logManager.newFailedRecordsHandler())
                      .map(recordMapper::map)
                      .transform(metricsManager.newFailedItemsMonitor())
                      .transform(logManager.newUnmappableStatementsHandler());
              if (batchingEnabled) {
                stmts =
                    stmts
                        .window(batchBufferSize)
                        .flatMap(batcher::batchByGroupingKey)
                        .transform(metricsManager.newBatcherMonitor());
              }
              return executeStatements(stmts, concurrency).subscribeOn(scheduler);
            },
            Runtime.getRuntime().availableProcessors());
  }

  @NotNull
  private Flux<Void> parallelFlux() {
    Scheduler scheduler1 = Schedulers.newSingle(new DefaultThreadFactory("workflow-publish"));
    Scheduler scheduler2 =
        Schedulers.newParallel(
            Runtime.getRuntime().availableProcessors(), new DefaultThreadFactory("workflow"));
    disposables.add(scheduler1);
    disposables.add(scheduler2);
    return Flux.defer(connector.readByResource())
        .flatMap(
            records ->
                Flux.from(records)
                    .window(batchingEnabled ? batchBufferSize : Queues.SMALL_BUFFER_SIZE))
        .publishOn(scheduler1, Queues.SMALL_BUFFER_SIZE * 4)
        .flatMap(
            records -> {
              Flux<Statement> stmts =
                  records
                      .transform(metricsManager.newTotalItemsMonitor())
                      .transform(logManager.newTotalItemsCounter())
                      .transform(metricsManager.newFailedItemsMonitor())
                      .transform(logManager.newFailedRecordsHandler())
                      .parallel()
                      .runOn(scheduler2)
                      .map(recordMapper::map)
                      .sequential()
                      .transform(metricsManager.newFailedItemsMonitor())
                      .transform(logManager.newUnmappableStatementsHandler());
              if (batchingEnabled) {
                stmts =
                    stmts
                        .transform(batcher::batchByGroupingKey)
                        .transform(metricsManager.newBatcherMonitor());
              }
              return executeStatements(stmts, maxInFlight);
            });
  }

  private Flux<Void> executeStatements(Flux<Statement> stmts, int concurrency) {
    Flux<WriteResult> results;
    if (dryRun) {
      results = stmts.map(s -> new DefaultWriteResult(s, null));
    } else {
      results = stmts.flatMap(executor::writeReactive, concurrency);
    }
    return results
        .transform(logManager.newFailedWritesHandler())
        .transform(logManager.newResultPositionTracker());
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(connector, e);
      if (disposables != null) {
        for (Disposable disposable : disposables) {
          e = WorkflowUtils.closeQuietly(disposable, e);
        }
      }
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(cluster, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      if (logManager != null) {
        logManager.reportLastLocations();
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
