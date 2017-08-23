/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.engine.internal.WorkflowUtils;
import com.datastax.loader.engine.internal.log.LogManager;
import com.datastax.loader.engine.internal.metrics.MetricsManager;
import com.datastax.loader.engine.internal.schema.RecordMapper;
import com.datastax.loader.engine.internal.settings.BatchSettings;
import com.datastax.loader.engine.internal.settings.CodecSettings;
import com.datastax.loader.engine.internal.settings.ConnectorSettings;
import com.datastax.loader.engine.internal.settings.DriverSettings;
import com.datastax.loader.engine.internal.settings.ExecutorSettings;
import com.datastax.loader.engine.internal.settings.LogSettings;
import com.datastax.loader.engine.internal.settings.MonitoringSettings;
import com.datastax.loader.engine.internal.settings.SchemaSettings;
import com.datastax.loader.engine.internal.settings.SettingsManager;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/** The main class for write workflows. */
public class WriteWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteWorkflow.class);

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.WRITE);
  private final LoaderConfig config;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;

  public WriteWorkflow(LoaderConfig config) throws Exception {
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
  }

  @Override
  public void execute() {

    LOGGER.info("Starting write workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect();
        Connector connector = connectorSettings.getConnector(WorkflowType.WRITE);
        MetricsManager metricsManager = monitoringSettings.newMetricsManager(WorkflowType.WRITE);
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
          .map(recordMapper::map)
          .compose(metricsManager.newRecordMapperMonitor())
          .compose(logManager.newRecordMapperErrorHandler())
          .compose(batchSettings.newStatementBatcher(cluster))
          .compose(metricsManager.newBatcherMonitor())
          .flatMap(executor::writeReactive)
          .compose(logManager.newWriteErrorHandler())
          .blockLast();

    } catch (Exception e) {
      LOGGER.error("Uncaught exception during write workflow engine execution " + executionId, e);
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info(
        "Write workflow engine execution {} finished in {}.",
        executionId,
        WorkflowUtils.formatElapsed(seconds));
  }
}
