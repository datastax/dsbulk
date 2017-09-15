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
import com.datastax.dsbulk.commons.config.DSBulkConfig;
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
import com.datastax.dsbulk.executor.api.writer.ReactiveBulkWriter;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for write workflows. */
public class LoadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadWorkflow.class);

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.LOAD);
  private final DSBulkConfig config;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;
  private EngineSettings engineSettings;

  LoadWorkflow(DSBulkConfig config) {
    this.config = config;
  }

  @Override
  public void init() throws Exception {
    SettingsManager settingsManager = new SettingsManager(config, executionId);
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

    LOGGER.info("Starting write workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    int maxMappingThreads = engineSettings.getMaxMappingThreads();
    Scheduler mapperScheduler = Schedulers.newParallel("record-mapper", maxMappingThreads);

    try (Connector connector = connectorSettings.getConnector(WorkflowType.LOAD);
        DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect();
        MetricsManager metricsManager = monitoringSettings.newMetricsManager(WorkflowType.LOAD);
        LogManager logManager = logSettings.newLogManager(cluster);
        ReactiveBulkWriter executor =
            executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener())) {

      connector.init();
      metricsManager.init();
      logManager.init();

      RecordMapper recordMapper =
          schemaSettings.createRecordMapper(
              session, connector.getRecordMetadata(), codecSettings.createCodecRegistry(cluster));

      Flux.from(connector.read())
          .parallel(maxMappingThreads)
          .runOn(mapperScheduler)
          .map(recordMapper::map)
          .composeGroup(metricsManager.newRecordMapperMonitor())
          .composeGroup(logManager.newRecordMapperErrorHandler())
          .composeGroup(batchSettings.newStatementBatcher(cluster))
          .composeGroup(metricsManager.newBatcherMonitor())
          .flatMap(executor::writeReactive)
          .composeGroup(logManager.newWriteErrorHandler())
          .sequential()
          .blockLast();

    } catch (Exception e) {
      System.err.printf(
          "Uncaught exception during write workflow engine execution %s: %s%n",
          executionId, e.getMessage());
      LOGGER.error("Uncaught exception during write workflow engine execution " + executionId, e);
    } finally {
      mapperScheduler.dispose();
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info(
        "Write workflow engine execution {} finished in {}.",
        executionId,
        WorkflowUtils.formatElapsed(seconds));
  }
}
