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
import com.datastax.loader.engine.internal.schema.ReadResultMapper;
import com.datastax.loader.engine.internal.settings.CodecSettings;
import com.datastax.loader.engine.internal.settings.ConnectorSettings;
import com.datastax.loader.engine.internal.settings.DriverSettings;
import com.datastax.loader.engine.internal.settings.ExecutorSettings;
import com.datastax.loader.engine.internal.settings.LogSettings;
import com.datastax.loader.engine.internal.settings.MonitoringSettings;
import com.datastax.loader.engine.internal.settings.SchemaSettings;
import com.datastax.loader.engine.internal.settings.SettingsManager;
import com.datastax.loader.executor.api.reader.RxJavaBulkReader;
import com.google.common.base.Stopwatch;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The main class for read workflows. */
public class ReadWorkflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadWorkflow.class);

  public static void main(String[] args) throws Exception {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    ReadWorkflow workflow = new ReadWorkflow(args);
    workflow.execute();
  }

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.READ);
  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final ExecutorSettings executorSettings;
  private final LogSettings logSettings;
  private final CodecSettings codecSettings;
  private final MonitoringSettings monitoringSettings;

  public ReadWorkflow(String[] args) throws Exception {
    SettingsManager settingsManager = new SettingsManager(args, executionId);
    settingsManager.loadConfiguration();
    settingsManager.logEffectiveSettings();
    logSettings = settingsManager.getLogSettings();
    driverSettings = settingsManager.getDriverSettings();
    connectorSettings = settingsManager.getConnectorSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    executorSettings = settingsManager.getExecutorSettings();
    codecSettings = settingsManager.getCodecSettings();
    monitoringSettings = settingsManager.getMonitoringSettings();
  }

  public void execute() {

    LOGGER.info("Starting read workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect();
        Connector connector = connectorSettings.getConnector();
        MetricsManager metricsManager = monitoringSettings.newMetricsManager();
        LogManager logManager = logSettings.newLogManager();
        RxJavaBulkReader executor =
            executorSettings.newReadExecutor(session, metricsManager.getExecutionListener())) {

      connector.init();
      metricsManager.init();
      logManager.init(cluster);

      RecordMetadata recordMetadata = connector.getRecordMetadata();
      ExtendedCodecRegistry codecRegistry = codecSettings.createCodecRegistry(cluster);
      ReadResultMapper readResultMapper =
          schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);

      Flowable.fromIterable(schemaSettings.createReadStatements(cluster))
          .flatMap(
              statement -> executor.readReactive(statement).subscribeOn(Schedulers.io()),
              executorSettings.getMaxConcurrentReads())
          .compose(logManager.newExecutorErrorHandler())
          .map(readResultMapper::map)
          .compose(metricsManager.newRecordMonitor())
          .compose(logManager.newRecordErrorHandler())
          .doOnNext(connector::write)
          .doOnNext(System.out::println) // TODO remove
          .blockingSubscribe();

    } catch (Exception e) {
      LOGGER.error("Uncaught exception during read workflow engine execution " + executionId, e);
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info(
        "Read workflow engine execution {} finished in {}.",
        executionId,
        WorkflowUtils.formatElapsed(seconds));
  }
}
