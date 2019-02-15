/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.INDEXED_RECORDS;
import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.MAPPED_RECORDS;
import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.printDebugInfoAboutCluster;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.checkProductCompatibility;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
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
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;
import com.google.common.base.Stopwatch;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Collections;
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

  private static final ExecutionInfo EMPTY_EXECUTION_INFO =
      new ExecutionInfo(0, 0, Collections.emptyList(), ONE, Collections.emptyMap());

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private DseCluster cluster;
  private ReactorBulkWriter executor;
  private boolean batchingEnabled;
  private boolean dryRun;
  private int resourceCount;
  private int batchBufferSize;
  private int writeConcurrency;
  private Scheduler scheduler;

  private Function<Record, Statement> mapper;
  private Function<Flux<Statement>, Flux<Statement>> batcher;
  private Function<Flux<Record>, Flux<Record>> totalItemsMonitor;
  private Function<Flux<Record>, Flux<Record>> totalItemsCounter;
  private Function<Flux<Record>, Flux<Record>> failedRecordsMonitor;
  private Function<Flux<Statement>, Flux<Statement>> failedStatementsMonitor;
  private Function<Flux<Record>, Flux<Record>> failedRecordsHandler;
  private Function<Flux<Statement>, Flux<Statement>> unmappableStatementsHandler;
  private Function<Flux<Statement>, Flux<Statement>> batcherMonitor;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private Function<Flux<WriteResult>, Flux<WriteResult>> failedWritesHandler;
  private Function<Flux<WriteResult>, Flux<Void>> resultPositionsHndler;
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
    driverSettings.init();
    executorSettings.init();
    engineSettings.init();
    cluster = driverSettings.newCluster();
    cluster.init();
    checkProductCompatibility(cluster);
    driverSettings.checkProtocolVersion(cluster);
    printDebugInfoAboutCluster(cluster);
    schemaSettings.init(
        WorkflowType.LOAD,
        cluster,
        connector.supports(INDEXED_RECORDS),
        connector.supports(MAPPED_RECORDS));
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
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(
            cluster.getConfiguration().getCodecRegistry(),
            schemaSettings.isAllowExtraFields(),
            schemaSettings.isAllowMissingFields());
    RecordMapper recordMapper =
        schemaSettings.createRecordMapper(session, connector.getRecordMetadata(), codecRegistry);
    mapper = recordMapper::map;
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(cluster)::batchByGroupingKey;
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

  private void threadPerCoreFlux() {
    Flux.defer(() -> connector.readByResource())
        .flatMap(
            records -> {
              Flux<Statement> stmts =
                  Flux.from(records)
                      .transform(totalItemsMonitor)
                      .transform(totalItemsCounter)
                      .transform(failedRecordsMonitor)
                      .transform(failedRecordsHandler)
                      .map(mapper)
                      .transform(failedStatementsMonitor)
                      .transform(unmappableStatementsHandler);
              if (batchingEnabled) {
                stmts = stmts.window(batchBufferSize).flatMap(batcher).transform(batcherMonitor);
              }
              return executeStatements(stmts)
                  .transform(failedWritesHandler)
                  .transform(resultPositionsHndler)
                  .subscribeOn(scheduler);
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
              Flux<Statement> stmts =
                  records
                      .transform(totalItemsMonitor)
                      .transform(totalItemsCounter)
                      .transform(failedRecordsMonitor)
                      .transform(failedRecordsHandler)
                      .map(mapper)
                      .transform(failedStatementsMonitor)
                      .transform(unmappableStatementsHandler);
              if (batchingEnabled) {
                stmts = stmts.transform(batcher).transform(batcherMonitor);
              }
              return executeStatements(stmts)
                  .transform(failedWritesHandler)
                  .transform(resultPositionsHndler)
                  .subscribeOn(scheduler);
            },
            numCores)
        .transform(terminationHandler)
        .blockLast();
  }

  private Flux<WriteResult> executeStatements(Flux<Statement> stmts) {
    Flux<WriteResult> results;
    if (dryRun) {
      results = stmts.map(stmt -> new DefaultWriteResult(stmt, EMPTY_EXECUTION_INFO));
    } else {
      results = stmts.flatMap(executor::writeReactive, writeConcurrency);
    }
    return results;
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
