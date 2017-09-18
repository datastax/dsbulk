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

/** The main class for read workflows. */
public class UnloadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final String executionId = WorkflowUtils.newExecutionId(WorkflowType.UNLOAD);
  private final LoaderConfig config;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;
  private EngineSettings engineSettings;

  UnloadWorkflow(LoaderConfig config) {
    this.config = config;
  }

  @Override
  public void init() throws Exception {
    SettingsManager settingsManager = new SettingsManager(config, executionId, WorkflowType.UNLOAD);
    settingsManager.loadConfiguration();
    settingsManager.logEffectiveSettings();
    logSettings = settingsManager.getLogSettings();
    driverSettings = settingsManager.getDriverSettings();
    connectorSettings = settingsManager.getConnectorSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    executorSettings = settingsManager.getExecutorSettings();
    codecSettings = settingsManager.getCodecSettings();
    monitoringSettings = settingsManager.getMonitoringSettings();
    engineSettings = settingsManager.getEngineSettings();
  }

  @Override
  public void execute() {

    LOGGER.info("Starting read workflow engine execution " + executionId);
    Stopwatch timer = Stopwatch.createStarted();

    int maxConcurrentReads = engineSettings.getMaxConcurrentReads();
    int maxMappingThreads = engineSettings.getMaxMappingThreads();

    Scheduler readsScheduler = Schedulers.newParallel("range-reads", maxConcurrentReads);
    Scheduler mapperScheduler = Schedulers.newParallel("result-mapper", maxMappingThreads);

    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.connect();
        Connector connector = connectorSettings.getConnector(WorkflowType.UNLOAD);
        MetricsManager metricsManager = monitoringSettings.newMetricsManager(WorkflowType.UNLOAD);
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

      Flux<Record> records =
          Flux.fromIterable(schemaSettings.createReadStatements(cluster))
              .flatMap(
                  statement -> executor.readReactive(statement).subscribeOn(readsScheduler),
                  maxConcurrentReads)
              .parallel(maxMappingThreads)
              .runOn(mapperScheduler)
              .composeGroup(logManager.newReadErrorHandler())
              .map(readResultMapper::map)
              .composeGroup(metricsManager.newResultMapperMonitor())
              .composeGroup(logManager.newResultMapperErrorHandler())
              .sequential()
              .publish()
              .autoConnect(2);

      // publish results to two subscribers: the connector,
      // and a dummy blockLast subscription to block until complete
      records.subscribe(connector.write());
      records.blockLast();

    } catch (Exception e) {
      System.err.printf(
          "Uncaught exception during read workflow engine execution %s: %s%n",
          executionId, e.getMessage());
      LOGGER.error("Uncaught exception during read workflow engine execution " + executionId, e);
    } finally {
      readsScheduler.dispose();
      mapperScheduler.dispose();
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info(
        "Read workflow engine execution {} finished in {}.",
        executionId,
        WorkflowUtils.formatElapsed(seconds));
  }
}
