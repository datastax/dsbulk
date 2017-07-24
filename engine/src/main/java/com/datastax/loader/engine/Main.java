/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.log.LogManager;
import com.datastax.loader.engine.internal.schema.RecordMapper;
import com.datastax.loader.engine.internal.settings.BatchSettings;
import com.datastax.loader.engine.internal.settings.ConnectorSettings;
import com.datastax.loader.engine.internal.settings.DriverSettings;
import com.datastax.loader.engine.internal.settings.ExecutorSettings;
import com.datastax.loader.engine.internal.settings.LogSettings;
import com.datastax.loader.engine.internal.settings.SchemaSettings;
import com.datastax.loader.engine.internal.settings.SettingsManager;
import com.datastax.loader.executor.api.result.Result;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.base.Stopwatch;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/** */
public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private final String operationId = newOperationId();

  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final BatchSettings batchSettings;
  private final ExecutorSettings executorSettings;
  private final LogSettings logSettings;

  public Main(String[] args) {
    SettingsManager settingsManager = new SettingsManager(args, operationId);
    logSettings = settingsManager.getLogSettings();
    driverSettings = settingsManager.getDriverSettings();
    connectorSettings = settingsManager.getConnectorSettings();
    schemaSettings = settingsManager.getSchemaSettings();
    batchSettings = settingsManager.getBatchSettings();
    executorSettings = settingsManager.getExecutorSettings();
    settingsManager.logEffectiveSettings();
    // TODO conversion, monitoring
  }

  public static void main(String[] args) {
    Main main = new Main(args);
    main.load();
  }

  public void load() {
    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.newSession();
        Connector connector = connectorSettings.newConnector();
        ReactiveBulkWriter engine = executorSettings.newWriteEngine(session);
        LogManager logManager = logSettings.newLogManager()) {

      connector.init();
      session.init();
      logManager.init(cluster);

      RecordMapper recordMapper = schemaSettings.newRecordMapper(session);
      FlowableTransformer<Statement, Statement> batcher =
          batchSettings.newStatementBatcher(cluster);
      FlowableTransformer<Record, Record> extractErrorHandler = logManager.newExtractErrorHandler();
      FlowableTransformer<Statement, Statement> transformErrorHandler =
          logManager.newTransformErrorHandler();
      FlowableTransformer<Result, Result> loadErrorHandler = logManager.newLoadErrorHandler();

      LOGGER.info("Starting operation " + operationId);
      Stopwatch timer = Stopwatch.createStarted();

      Flowable.fromPublisher(connector.read())
          .compose(extractErrorHandler)
          .map(recordMapper::map)
          .compose(transformErrorHandler)
          .compose(batcher)
          .flatMap(engine::writeReactive)
          .compose(loadErrorHandler)
          .blockingSubscribe();

      // TODO monitoring

      timer.stop();
      long seconds = timer.elapsed(SECONDS);
      LOGGER.info("Operation {} finished in {}.", operationId, formatElapsed(seconds));

    } catch (Exception e) {
      LOGGER.error(
          String.format("Operation %s: uncaught exception during engine execution.", operationId),
          e);
    }
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
