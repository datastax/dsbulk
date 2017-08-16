/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.loader.connectors.api.Connector;
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
import com.datastax.loader.engine.internal.url.MainURLStreamHandlerFactory;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.base.Stopwatch;
import io.reactivex.Flowable;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    URL.setURLStreamHandlerFactory(new MainURLStreamHandlerFactory());
    Main main = new Main(args);
    main.load();
  }

  private final String operationId = newOperationId();
  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final BatchSettings batchSettings;
  private final ExecutorSettings executorSettings;
  private final LogSettings logSettings;
  private final CodecSettings codecSettings;
  private final MonitoringSettings monitoringSettings;

  public Main(String[] args) throws Exception {
    SettingsManager settingsManager = new SettingsManager(args, operationId);
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

  public void load() {

    LOGGER.info("Starting operation " + operationId);
    Stopwatch timer = Stopwatch.createStarted();

    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.newSession();
        Connector connector = connectorSettings.getConnector();
        MetricsManager metricsManager = monitoringSettings.newMetricsManager();
        LogManager logManager = logSettings.newLogManager();
        ReactiveBulkWriter executor =
            executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener())) {

      session.init();
      connector.init();
      metricsManager.init();
      logManager.init(cluster);

      ExtendedCodecRegistry codecRegistry = codecSettings.init(cluster);
      RecordMapper recordMapper = schemaSettings.init(session, codecRegistry);

      Flowable.fromPublisher(connector.read())
          .compose(metricsManager.newConnectorMonitor())
          .compose(logManager.newConnectorErrorHandler())
          .map(recordMapper::map)
          .compose(metricsManager.newMapperMonitor())
          .compose(logManager.newMapperErrorHandler())
          .compose(batchSettings.newStatementBatcher(cluster))
          .compose(metricsManager.newBatcherMonitor())
          .flatMap(executor::writeReactive)
          .compose(logManager.newExecutorErrorHandler())
          .blockingSubscribe();

    } catch (Exception e) {
      LOGGER.error(
          String.format(
              "Operation %s: uncaught exception during workflow engine execution.", operationId),
          e);
    }

    timer.stop();
    long seconds = timer.elapsed(SECONDS);
    LOGGER.info("Operation {} finished in {}.", operationId, formatElapsed(seconds));
  }

  public void unload() {
    // TODO
  }

  private static String newOperationId() {
    return "load_"
        + DateTimeFormatter.ofPattern("uuuu_MM_dd_HH_mm_ss_nnnnnnnnn")
            .format(
                ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("UTC"))
                    .with(ChronoField.NANO_OF_SECOND, new Random().nextInt(1_000_000_000)));
  }

  private static String formatElapsed(long seconds) {
    long hr = SECONDS.toHours(seconds);
    long min = SECONDS.toMinutes(seconds - HOURS.toSeconds(hr));
    long sec = SECONDS.toSeconds(seconds - HOURS.toSeconds(hr) - MINUTES.toSeconds(min));
    if (hr > 0) return String.format("%d hours, %d minutes and %d seconds", hr, min, sec);
    else if (min > 0) return String.format("%d minutes and %d seconds", min, sec);
    else return String.format("%d seconds", sec);
  }
}
