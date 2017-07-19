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
import com.datastax.loader.engine.schema.RecordMapper;
import com.datastax.loader.engine.settings.BatchSettings;
import com.datastax.loader.engine.settings.ConnectorSettings;
import com.datastax.loader.engine.settings.DriverSettings;
import com.datastax.loader.engine.settings.ExecutorSettings;
import com.datastax.loader.engine.settings.SchemaSettings;
import com.datastax.loader.engine.settings.SettingsManager;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.typesafe.config.Config;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/** */
public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final BatchSettings batchSettings;
  private final ExecutorSettings executorSettings;

  public static void main(String[] args) {
    Main main = new Main(args);
    main.load();
  }

  @VisibleForTesting
  Main(String[] args) {
    SettingsManager settingsManager = new SettingsManager();
    Config config = settingsManager.loadConfig(args);
    driverSettings = new DriverSettings(config.getConfig("connection"));
    connectorSettings = new ConnectorSettings(config.getConfig("connector"));
    schemaSettings = new SchemaSettings(config.getConfig("schema"));
    batchSettings = new BatchSettings(config.getConfig("batch"));
    executorSettings = new ExecutorSettings(config.getConfig("executor"));
    // TODO conversion, error handling, monitoring
  }

  @VisibleForTesting
  void load() {
    try (DseCluster cluster = driverSettings.newCluster();
        DseSession session = cluster.newSession();
        Connector connector = connectorSettings.newConnector();
        ReactiveBulkWriter engine = executorSettings.newWriteEngine(session)) {

      connector.init();
      session.init();

      RecordMapper recordMapper = schemaSettings.newRecordMapper(session);
      FlowableTransformer<Statement, Statement> batcher =
          batchSettings.newStatementBatcher(cluster);

      LOGGER.info("Starting");
      Stopwatch timer = Stopwatch.createStarted();

      Flowable.fromPublisher(connector.read())
          .map(recordMapper::map)
          .compose(batcher)
          .flatMap(engine::writeReactive)
          .blockingSubscribe();

      // TODO connector bad file
      // TODO executor bad file
      // TODO monitoring

      timer.stop();
      long seconds = timer.elapsed(SECONDS);
      LOGGER.info("Finished in {} ", formatElapsed(seconds));

    } catch (Exception e) {
      LOGGER.error(
          "Uncaught exception during engine execution. "
              + "This is most likely a bug in this tool, please report.",
          e);
    }
  }

  @VisibleForTesting
  void unload() {
    // TODO
  }

  private static String formatElapsed(long seconds) {
    long hr = SECONDS.toHours(seconds);
    long min = SECONDS.toMinutes(seconds - HOURS.toSeconds(hr));
    long sec = SECONDS.toSeconds(seconds - HOURS.toSeconds(hr) - MINUTES.toSeconds(min));
    return String.format("%02d hours, %02d minutes and %02d seconds", hr, min, sec);
  }
}
