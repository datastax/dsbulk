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
import com.datastax.loader.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.engine.internal.WorkflowUtils;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
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
import io.reactivex.Flowable;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The main class for write workflows. */
public class WriteWorkflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteWorkflow.class);

  public static void main(String[] args) throws Exception {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    WriteWorkflow writeWorkflow = new WriteWorkflow(args);
    writeWorkflow.execute();
  }

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.WRITE);
  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final BatchSettings batchSettings;
  private final ExecutorSettings executorSettings;
  private final LogSettings logSettings;
  private final CodecSettings codecSettings;
  private final MonitoringSettings monitoringSettings;

  public WriteWorkflow(String[] args) throws Exception {
    SettingsManager settingsManager = new SettingsManager(args, executionId);
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

  public void execute() {

    LOGGER.info("Starting write workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect();
        Connector connector = connectorSettings.getConnector();
        MetricsManager metricsManager = monitoringSettings.newMetricsManager();
        LogManager logManager = logSettings.newLogManager();
        ReactiveBulkWriter executor =
            executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener())) {

      connector.init();
      metricsManager.init();
      logManager.init(cluster);

      RecordMetadata recordMetadata = connector.getRecordMetadata();
      ExtendedCodecRegistry codecRegistry = codecSettings.createCodecRegistry(cluster);
      RecordMapper recordMapper =
          schemaSettings.createRecordMapper(session, recordMetadata, codecRegistry);

      Flowable.fromPublisher(connector.read())
          .compose(metricsManager.newRecordMonitor())
          .compose(logManager.newRecordErrorHandler())
          .map(recordMapper::map)
          .compose(metricsManager.newMapperMonitor())
          .compose(logManager.newMapperErrorHandler())
          .compose(batchSettings.newStatementBatcher(cluster))
          .compose(metricsManager.newBatcherMonitor())
          .flatMap(executor::writeReactive)
          .compose(logManager.newExecutorErrorHandler())
          .blockingSubscribe();

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
