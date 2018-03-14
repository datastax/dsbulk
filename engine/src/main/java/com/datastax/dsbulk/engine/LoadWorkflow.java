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
  private DefaultThreadFactory threadFactory;

  LoadWorkflow(LoaderConfig config) {
    settingsManager = new SettingsManager(config, WorkflowType.LOAD);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init();
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    logSettings.init(false);
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    connectorSettings.init();
    settingsManager.logEffectiveSettings(
        connectorSettings.getConnectorName(), connectorSettings.getConnectorConfig());
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
    schemaSettings.init(codecSettings.getTimestampCodec());
    executorSettings.init();
    engineSettings.init();
    connector = connectorSettings.getConnector();
    connector.init();
    cluster = driverSettings.newCluster();
    checkProductCompatibility(cluster);
    String keyspace = schemaSettings.getKeyspace();
    DseSession session = cluster.connect(keyspace);
    batchingEnabled = batchSettings.isBatchingEnabled();
    batchBufferSize = batchSettings.getBufferSize();
    logManager = logSettings.newLogManager(WorkflowType.LOAD, cluster);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.LOAD,
            batchingEnabled,
            logManager.getExecutionDirectory(),
            cluster.getMetrics().getRegistry(),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
    metricsManager.init();
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    recordMapper =
        schemaSettings.createRecordMapper(
            session, connector.getRecordMetadata(), codecSettings.createCodecRegistry(cluster));
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(cluster);
    }
    maxInFlight = executorSettings.getMaxInFlight();
    dryRun = engineSettings.isDryRun();
    closed.set(false);
    resourceCount = connector.estimatedResourceCount();
    disposables = new HashSet<>();
    threadFactory = new DefaultThreadFactory("workflow");
  }

  @Override
  public boolean execute() {
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
    if (logManager.getTotalErrors() == 0) {
      LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
    } else {
      LOGGER.info(
          "{} completed with {} errors in {}. Check log files under {} for details.",
          this,
          logManager.getTotalErrors(),
          WorkflowUtils.formatElapsed(seconds),
          logManager.getExecutionDirectory());
    }
    return logManager.getTotalErrors() == 0;
  }

  @NotNull
  private Flux<Void> threadPerCoreFlux() {
    LOGGER.info("Using thread-per-core pattern.");
    Scheduler scheduler =
        Schedulers.newParallel(Runtime.getRuntime().availableProcessors(), threadFactory);
    disposables.add(scheduler);
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
                      .transform(logManager.newFailedStatementsHandler());
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
    Scheduler scheduler1 = Schedulers.newSingle(threadFactory);
    Scheduler scheduler2 =
        Schedulers.newParallel(Runtime.getRuntime().availableProcessors(), threadFactory);
    disposables.add(scheduler1);
    disposables.add(scheduler2);
    return Flux.defer(connector.readByResource())
        .flatMap(records -> Flux.from(records).window(batchBufferSize))
        .publishOn(scheduler1, Queues.SMALL_BUFFER_SIZE * 4)
        .parallel()
        .runOn(scheduler2)
        .flatMap(
            records ->
                records
                    .transform(metricsManager.newTotalItemsMonitor())
                    .transform(logManager.newTotalItemsCounter())
                    .transform(metricsManager.newFailedItemsMonitor())
                    .transform(logManager.newFailedRecordsHandler())
                    .map(recordMapper::map)
                    .transform(metricsManager.newFailedItemsMonitor())
                    .transform(logManager.newFailedStatementsHandler())
                    .transform(batcher::batchByGroupingKey)
                    .transform(metricsManager.newBatcherMonitor())
                    .transform(this::executeStatements))
        .sequential();
  }

  @NotNull
  private Flux<Void> parallelUnbatchedFlux() {
    Scheduler scheduler1 = Schedulers.newSingle(threadFactory);
    Scheduler scheduler2 =
        Schedulers.newParallel(Runtime.getRuntime().availableProcessors(), threadFactory);
    disposables.add(scheduler1);
    disposables.add(scheduler2);
    return Flux.defer(connector.read())
        .publishOn(scheduler1, Queues.SMALL_BUFFER_SIZE * 4)
        .parallel()
        .runOn(scheduler2)
        .composeGroup(metricsManager.newTotalItemsMonitor())
        .composeGroup(logManager.newTotalItemsCounter())
        .composeGroup(metricsManager.newFailedItemsMonitor())
        .composeGroup(logManager.newFailedRecordsHandler())
        .map(recordMapper::map)
        .composeGroup(metricsManager.newFailedItemsMonitor())
        .composeGroup(logManager.newFailedStatementsHandler())
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
        .transform(logManager.newFailedWritesHandler())
        .transform(logManager.newResultPositionTracker());
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.info("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(connector, e);
      if (disposables != null) {
        for (Disposable disposable : disposables) {
          e = WorkflowUtils.closeQuietly(disposable, e);
        }
      }
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
