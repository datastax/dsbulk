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

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;
  private EngineSettings engineSettings;

  LoadWorkflow(LoaderConfig config) {
    this.config = config;
  }

  @Override
  public void init() throws Exception {
    SettingsManager settingsManager = new SettingsManager(config, executionId, WorkflowType.LOAD);
    settingsManager.loadConfiguration();
    settingsManager.logEffectiveSettings();
    logSettings = settingsManager.getLogSettings();
    driverSettings = settingsManager.getDriverSettings();
    connectorSettings = settingsManager.getConnectorSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    batchSettings = settingsManager.getBatchSettings();
    executorSettings = settingsManager.getExecutorSettings();
    codecSettings = settingsManager.getCodecSettings();
    monitoringSettings = settingsManager.getMonitoringSettings();
    engineSettings = settingsManager.getEngineSettings();
  }

  @Override
  public void execute() {

    LOGGER.info("Starting load workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    int maxConcurrentWrites = executorSettings.getMaxConcurrentOps();
    int maxMappingThreads = engineSettings.getMaxMappingThreads();

    Scheduler writesScheduler = Schedulers.newParallel("batch-writes", maxConcurrentWrites);
    Scheduler mapperScheduler = Schedulers.newParallel("record-mapper", maxMappingThreads);
    String keyspace = config.getString("schema.keyspace");
    if (keyspace != null && keyspace.isEmpty()) {
      keyspace = null;
    }
    try (Connector connector = connectorSettings.getConnector(WorkflowType.LOAD);
        DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect(keyspace);
        MetricsManager metricsManager = monitoringSettings.newMetricsManager(WorkflowType.LOAD);
        LogManager logManager = logSettings.newLogManager(cluster);
        ReactorBulkWriter executor =
            executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener())) {

      connector.init();
      metricsManager.init();
      logManager.init();

      RecordMapper recordMapper =
          schemaSettings.createRecordMapper(
              session, connector.getRecordMetadata(), codecSettings.createCodecRegistry(cluster));
      ReactorUnsortedStatementBatcher batcher = batchSettings.newStatementBatcher(cluster);

      Flux.from(connector.read())
          .parallel(maxMappingThreads)
          .runOn(mapperScheduler)
          .map(recordMapper::map)
          .sequential()
          .compose(metricsManager.newRecordMapperMonitor())
          .compose(logManager.newRecordMapperErrorHandler())
          .compose(batcher)
          .compose(metricsManager.newBatcherMonitor())
          .flatMap(
              statement -> executor.writeReactive(statement).subscribeOn(writesScheduler),
              maxConcurrentWrites)
          .compose(logManager.newWriteErrorHandler())
          .blockLast();

    } catch (Exception e) {
      System.err.printf(
          "Uncaught exception during load workflow engine execution %s: %s%n",
          executionId, e.getMessage());
      LOGGER.error("Uncaught exception during load workflow engine execution " + executionId, e);
    } finally {
      writesScheduler.dispose();
      mapperScheduler.dispose();
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info(
        "Load workflow engine execution {} finished in {}.",
        executionId,
        WorkflowUtils.formatElapsed(seconds));
  }
}
