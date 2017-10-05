/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static java.util.concurrent.TimeUnit.SECONDS;

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
import com.datastax.dsbulk.executor.api.writer.ReactorBulkWriter;
import com.google.common.base.Stopwatch;
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

  private Scheduler mapperScheduler;
  private RecordMapper recordMapper;
  private ReactorUnsortedStatementBatcher batcher;
  private ReactorBulkWriter executor;
  private Connector connector;
  private int maxConcurrentMappings;
  private int mappingsBufferSize;
  private MetricsManager metricsManager;
  private LogManager logManager;

  private volatile boolean closed = false;

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
    mappingsBufferSize = engineSettings.getMappingsBufferSize();
    mapperScheduler = Schedulers.newElastic("record-mapper");
    connector = connectorSettings.getConnector(WorkflowType.LOAD);
    connector.init();
    DseCluster cluster = driverSettings.newCluster();
    String keyspace = schemaSettings.getKeyspace();
    DseSession session = cluster.connect(keyspace);
    metricsManager = monitoringSettings.newMetricsManager(WorkflowType.LOAD);
    metricsManager.init();
    logManager = logSettings.newLogManager(cluster);
    logManager.init();
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    recordMapper =
        schemaSettings.createRecordMapper(
            session, connector.getRecordMetadata(), codecSettings.createCodecRegistry(cluster));
    batcher = batchSettings.newStatementBatcher(cluster);
  }

  @Override
  public void execute() {
    LOGGER.info("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux.from(connector.read())
        .parallel(maxConcurrentMappings, mappingsBufferSize)
        .runOn(mapperScheduler)
        .map(recordMapper::map)
        .sequential()
        .compose(metricsManager.newRecordMapperMonitor())
        .compose(logManager.newRecordMapperErrorHandler())
        .compose(batcher)
        .compose(metricsManager.newBatcherMonitor())
        .flatMap(executor::writeReactive)
        .compose(logManager.newWriteErrorHandler())
        .compose(logManager.newLocationTracker())
        .blockLast();
    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      LOGGER.info("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(connector, null);
      e = WorkflowUtils.closeQuietly(mapperScheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(metricsManager, e);
      e = WorkflowUtils.closeQuietly(logManager, e);
      closed = true;
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
