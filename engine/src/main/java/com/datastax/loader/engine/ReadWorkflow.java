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
import com.datastax.loader.connectors.api.Record;
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
import com.datastax.loader.executor.api.reader.ReactorBulkReader;
import com.google.common.base.Stopwatch;
import java.net.URL;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
        Connector connector = connectorSettings.getConnector(WorkflowType.READ);
        MetricsManager metricsManager = monitoringSettings.newMetricsManager(WorkflowType.READ);
        LogManager logManager = logSettings.newLogManager(cluster);
        ReactorBulkReader executor =
            executorSettings.newReadExecutor(session, metricsManager.getExecutionListener())) {

      connector.init();
      metricsManager.init();
      logManager.init();

      RecordMetadata recordMetadata = connector.getRecordMetadata();
      ExtendedCodecRegistry codecRegistry = codecSettings.createCodecRegistry(cluster);
      ReadResultMapper readResultMapper =
          schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
      ThreadPoolExecutor threadPool = executorSettings.getExecutorThreadPool();
      Scheduler executorScheduler = Schedulers.fromExecutor(threadPool);
      int maxConcurrentReads = executorSettings.getMaxConcurrentReads();

      Flux<Record> records =
          Flux.fromIterable(schemaSettings.createReadStatements(cluster))
              .flatMap(
                  statement -> executor.readReactive(statement).subscribeOn(executorScheduler),
                  maxConcurrentReads)
              .compose(logManager.newReadErrorHandler())
              .map(readResultMapper::map)
              .compose(metricsManager.newResultMapperMonitor())
              .compose(logManager.newResultMapperErrorHandler())
              .publish()
              .autoConnect(2);

      // publish results to two subscribers: the connector,
      // and a dummy blockLast subscription to block until complete
      records.subscribe(connector.write());
      records.blockLast();

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
