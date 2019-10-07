/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.INDEXED_RECORDS;
import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.MAPPED_RECORDS;
import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.printDebugInfoAboutCluster;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.checkProductCompatibility;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
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
import com.datastax.dsbulk.executor.api.internal.result.EmptyWriteResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private MetricsManager metricsManager;
  private LogManager logManager;
  private DseSession session;
  private ReactorBulkWriter executor;
  private boolean batchingEnabled;
  private boolean dryRun;
  private int resourceCount;
  private int batchBufferSize;
  private int writeConcurrency;
  private Scheduler scheduler;

  private Function<Record, BatchableStatement<?>> mapper;
  private Function<Flux<BatchableStatement<?>>, Flux<Statement<?>>> batcher;
  private Function<Flux<Record>, Flux<Record>> totalItemsMonitor;
  private Function<Flux<Record>, Flux<Record>> totalItemsCounter;
  private Function<Flux<Record>, Flux<Record>> failedRecordsMonitor;
  private Function<Flux<BatchableStatement<?>>, Flux<BatchableStatement<?>>>
      failedStatementsMonitor;
  private Function<Flux<Record>, Flux<Record>> failedRecordsHandler;
  private Function<Flux<BatchableStatement<?>>, Flux<BatchableStatement<?>>>
      unmappableStatementsHandler;
  private Function<Flux<Statement<?>>, Flux<Statement<?>>> batcherMonitor;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private Function<Flux<WriteResult>, Flux<WriteResult>> failedWritesHandler;
  private Function<Flux<WriteResult>, Flux<Void>> resultPositionsHndler;
  private Function<Flux<WriteResult>, Flux<WriteResult>> queryWarningsHandler;
  private int numCores;

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
    executorSettings.init();
    engineSettings.init();
    driverSettings.init(executorSettings.getExecutorConfig());
    session = driverSettings.newSession(executionId);
    checkProductCompatibility(session);
    printDebugInfoAboutCluster(session);
    schemaSettings.init(
        WorkflowType.LOAD,
        session,
        connector.supports(INDEXED_RECORDS),
        connector.supports(MAPPED_RECORDS));
    batchingEnabled = batchSettings.isBatchingEnabled();
    batchBufferSize = batchSettings.getBufferSize();
    logManager = logSettings.newLogManager(WorkflowType.LOAD, session);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.LOAD,
            batchingEnabled,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getMetrics().map(Metrics::getRegistry).orElse(new MetricRegistry()),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry());
    metricsManager.init();
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, connector.getRecordMetadata(), codecRegistry);
    mapper = recordMapper::map;
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(session)::batchByGroupingKey;
    }
    dryRun = engineSettings.isDryRun();
    if (dryRun) {
      LOGGER.info("Dry-run mode enabled.");
    }
    closed.set(false);
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedRecordsMonitor = metricsManager.newFailedItemsMonitor();
    failedStatementsMonitor = metricsManager.newFailedItemsMonitor();
    batcherMonitor = metricsManager.newBatcherMonitor();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedRecordsHandler = logManager.newFailedRecordsHandler();
    unmappableStatementsHandler = logManager.newUnmappableStatementsHandler();
    queryWarningsHandler = logManager.newQueryWarningsHandler();
    failedWritesHandler = logManager.newFailedWritesHandler();
    resultPositionsHndler = logManager.newResultPositionsHandler();
    terminationHandler = logManager.newTerminationHandler();
    numCores = Runtime.getRuntime().availableProcessors();
    scheduler = Schedulers.newParallel(numCores, new DefaultThreadFactory("workflow"));
    // In order to keep a global number of X in-flight requests maximum, and in order to reduce lock
    // contention around the semaphore that controls this number, and given that we have N threads
    // executing requests, then each thread should strive to maintain a maximum of X / N in-flight
    // requests. If the maximum number of in-flight requests is unbounded, then we use a standard
    // concurrency constant.
    writeConcurrency =
        executorSettings.getMaxInFlight().isPresent()
            ? Math.max(Queues.XS_BUFFER_SIZE, executorSettings.getMaxInFlight().get() / numCores)
            : Queues.XS_BUFFER_SIZE;
    resourceCount = connector.estimatedResourceCount();
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    if (resourceCount >= WorkflowUtils.TPC_THRESHOLD) {
      threadPerCoreFlux();
    } else {
      parallelFlux();
    }
    timer.stop();
    metricsManager.stop();
    long seconds = timer.elapsed(SECONDS);
    if (logManager.getTotalErrors() == 0) {
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

  private void threadPerCoreFlux() {
    Flux.defer(() -> connector.readByResource())
        .flatMap(
            records -> {
              Flux<BatchableStatement<?>> stmts =
                  Flux.from(records)
                      .transform(totalItemsMonitor)
                      .transform(totalItemsCounter)
                      .transform(failedRecordsMonitor)
                      .transform(failedRecordsHandler)
                      .map(mapper)
                      .transform(failedStatementsMonitor)
                      .transform(unmappableStatementsHandler);
              Flux<? extends Statement<?>> grouped;
              if (batchingEnabled) {
                grouped = stmts.window(batchBufferSize).flatMap(batcher).transform(batcherMonitor);
              } else {
                grouped = stmts;
              }
              return executeStatements(grouped).subscribeOn(scheduler);
            },
            numCores)
        .transform(terminationHandler)
        .blockLast();
  }

  private void parallelFlux() {
    Flux.defer(() -> connector.read())
        .window(batchingEnabled ? batchBufferSize : Queues.SMALL_BUFFER_SIZE)
        .flatMap(
            records -> {
              Flux<BatchableStatement<?>> stmts =
                  records
                      .transform(totalItemsMonitor)
                      .transform(totalItemsCounter)
                      .transform(failedRecordsMonitor)
                      .transform(failedRecordsHandler)
                      .map(mapper)
                      .transform(failedStatementsMonitor)
                      .transform(unmappableStatementsHandler);
              Flux<? extends Statement<?>> grouped;
              if (batchingEnabled) {
                grouped = stmts.transform(batcher).transform(batcherMonitor);
              } else {
                grouped = stmts;
              }
              return executeStatements(grouped).subscribeOn(scheduler);
            },
            numCores)
        .transform(terminationHandler)
        .blockLast();
  }

  private Flux<Void> executeStatements(Flux<? extends Statement<?>> stmts) {
    Flux<WriteResult> results;
    if (dryRun) {
      results = stmts.map(EmptyWriteResult::new);
    } else {
      results = stmts.flatMap(executor::writeReactive, writeConcurrency);
    }
    return results
        .transform(queryWarningsHandler)
        .transform(failedWritesHandler)
        .transform(resultPositionsHndler);
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
      e = WorkflowUtils.closeQuietly(session, e);
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
    if (executionId == null) {
      return "Operation";
    } else {
      return "Operation " + executionId;
    }
  }
}
