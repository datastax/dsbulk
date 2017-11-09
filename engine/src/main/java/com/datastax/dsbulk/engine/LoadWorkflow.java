/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
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
import com.datastax.dsbulk.executor.api.batch.ReactorUnsortedStatementBatcher;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.writer.ReactorBulkWriter;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
  private ReactorUnsortedStatementBatcher batcher;
  private DseCluster cluster;
  private ReactorBulkWriter executor;
  private int maxConcurrentMappings;
  private int maxConcurrentWrites;
  private int bufferSize;
  private boolean batchingEnabled;
  private boolean dryRun;
  private int threadPerCoreThreshold;
  private long resourceCount;

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
    maxConcurrentMappings = engineSettings.getMaxConcurrentMappings();
    maxConcurrentWrites = executorSettings.getMaxInFlight();
    bufferSize = engineSettings.getBufferSize();
    dryRun = engineSettings.isDryRun();
    scheduler = Schedulers.newElastic("workflow");
    connector = connectorSettings.getConnector();
    connector.init();
    cluster = driverSettings.newCluster();
    String keyspace = schemaSettings.getKeyspace();
    DseSession session = cluster.connect(keyspace);
    checkProductCompatibility(cluster);
    batchingEnabled = batchSettings.isBatchingEnabled();
    metricsManager = monitoringSettings.newMetricsManager(WorkflowType.LOAD, batchingEnabled);
    metricsManager.init();
    logManager = logSettings.newLogManager(WorkflowType.LOAD, cluster);
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
    threadPerCoreThreshold = engineSettings.getThreadPerCoreThreshold();
  }

  @Override
  public void execute() throws InterruptedException {
    LOGGER.info("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Void> flux;
    if (resourceCount > threadPerCoreThreshold) {
      LOGGER.info("Using thread-per-core pattern.");
      flux =
          Flux.from(connector.readByResource())
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
                          stmts.transform(batcher).transform(metricsManager.newBatcherMonitor());
                    }
                    Flux<WriteResult> results;
                    if (dryRun) {
                      results = stmts.map(s -> new DefaultWriteResult(s, null));
                    } else {
                      results = stmts.flatMap(executor::writeReactive, maxConcurrentWrites);
                    }
                    return results
                        .transform(logManager.newWriteErrorHandler())
                        .transform(logManager.newResultPositionTracker(scheduler))
                        .subscribeOn(scheduler);
                  },
                  maxConcurrentMappings);
    } else {
      Flux<Statement> stmts =
          Flux.from(connector.read())
              .publishOn(scheduler, bufferSize)
              .transform(metricsManager.newUnmappableRecordMonitor())
              .transform(logManager.newUnmappableRecordErrorHandler())
              .parallel(maxConcurrentMappings)
              .runOn(scheduler)
              .map(recordMapper::map)
              .sequential()
              .transform(metricsManager.newUnmappableStatementMonitor())
              .transform(logManager.newUnmappableStatementErrorHandler());
      if (batchingEnabled) {
        stmts = stmts.transform(batcher).transform(metricsManager.newBatcherMonitor());
      }
      Flux<WriteResult> results;
      if (dryRun) {
        results = stmts.map(s -> new DefaultWriteResult(s, null));
      } else {
        results = stmts.flatMap(executor::writeReactive, maxConcurrentWrites);
      }
      flux =
          results
              .transform(logManager.newWriteErrorHandler())
              .transform(logManager.newResultPositionTracker(scheduler));
    }
    flux.blockLast();
    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
  }

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

  private static void checkProductCompatibility(Cluster cluster) {
    Set<Host> hosts = cluster.getMetadata().getAllHosts();
    List<Host> nonDseHosts =
        hosts.stream().filter(host -> host.getDseVersion() == null).collect(Collectors.toList());
    if (nonDseHosts.size() != 0) {
      LOGGER.error(
          "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster.");
      LOGGER.error("The following nodes do not appear to be running DSE:");
      for (Host host : nonDseHosts) {
        LOGGER.error(host.toString());
      }
      throw new IllegalStateException("Unable to load data to non DSE cluster");
    }
  }

  @Override
  public String toString() {
    return "Load workflow engine execution " + executionId;
  }
}
