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
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.WorkflowUtils;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.settings.ConnectorSettings;
import com.datastax.dsbulk.engine.internal.settings.DriverSettings;
import com.datastax.dsbulk.engine.internal.settings.EngineSettings;
import com.datastax.dsbulk.engine.internal.settings.ExecutorSettings;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.settings.MonitoringSettings;
import com.datastax.dsbulk.engine.internal.settings.SchemaSettings;
import com.datastax.dsbulk.engine.internal.settings.SettingsManager;
import com.datastax.dsbulk.executor.api.reader.ReactorBulkReader;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for unload workflows. */
public class UnloadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.UNLOAD);
  private final LoaderConfig config;

  private SchemaSettings schemaSettings;
  private Scheduler readsScheduler;
  private Scheduler mapperScheduler;
  private Connector connector;
  private MetricsManager metricsManager;
  private ReactorBulkReader executor;
  private LogManager logManager;
  private ReadResultMapper readResultMapper;
  private DseCluster cluster;
  private int maxConcurrentReads;
  private int maxConcurrentMappings;
  private int mappingsBufferSize;
  private int readsBufferSize;

  private volatile boolean closed = false;

  UnloadWorkflow(LoaderConfig config) {
    this.config = config;
  }

  @Override
  public void init() throws Exception {
    SettingsManager settingsManager = new SettingsManager(config, executionId, WorkflowType.UNLOAD);
    settingsManager.loadConfiguration();
    settingsManager.logEffectiveSettings();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    maxConcurrentReads = engineSettings.getMaxConcurrentOps();
    maxConcurrentMappings = engineSettings.getMaxConcurrentMappings();
    mappingsBufferSize = engineSettings.getMappingsBufferSize();
    readsBufferSize = engineSettings.getOpsBufferSize();
    readsScheduler = Schedulers.newElastic("range-reads");
    mapperScheduler = Schedulers.newElastic("result-mapper");
    connector = connectorSettings.getConnector(WorkflowType.UNLOAD);
    connector.init();
    cluster = driverSettings.newCluster();
    String keyspace = schemaSettings.getKeyspace();
    DseSession session = cluster.connect(keyspace);
    metricsManager = monitoringSettings.newMetricsManager(WorkflowType.UNLOAD);
    metricsManager.init();
    logManager = logSettings.newLogManager(cluster);
    logManager.init();
    executor = executorSettings.newReadExecutor(session, metricsManager.getExecutionListener());
    RecordMetadata recordMetadata = connector.getRecordMetadata();
    ExtendedCodecRegistry codecRegistry = codecSettings.createCodecRegistry(cluster);
    readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
  }

  @Override
  public void execute() {
    LOGGER.info("{} started.", this);
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Record> records =
        Flux.fromIterable(schemaSettings.createReadStatements(cluster))
            .flatMap(
                statement -> executor.readReactive(statement).subscribeOn(readsScheduler),
                maxConcurrentReads,
                readsBufferSize)
            .compose(logManager.newReadErrorHandler())
            .parallel(maxConcurrentMappings, mappingsBufferSize)
            .runOn(mapperScheduler)
            .map(readResultMapper::map)
            .sequential(1024) // FIXME should this one be configurable?
            .compose(metricsManager.newResultMapperMonitor())
            .compose(logManager.newResultMapperErrorHandler())
            .publish(1024) // FIXME should this one be configurable?
            .autoConnect(2);
    // publish results to two subscribers: the connector,
    // and a dummy blockLast subscription to block until complete
    records.subscribe(connector.write());
    records.blockLast();
    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info("{} completed successfully in {}.", this, WorkflowUtils.formatElapsed(seconds));
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      LOGGER.info("{} closing.", this);
      Exception e = WorkflowUtils.closeQuietly(connector, null);
      e = WorkflowUtils.closeQuietly(readsScheduler, e);
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
    return "Unload workflow engine execution " + executionId;
  }
}
