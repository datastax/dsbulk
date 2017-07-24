/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SettingsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsManager.class);

  private static final Config REFERENCE =
      ConfigFactory.defaultReference().getConfig("datastax-loader");

  private static final Config DEFAULT = ConfigFactory.load().getConfig("datastax-loader");

  private final Config config;
  private final DriverSettings driverSettings;
  private final ConnectorSettings connectorSettings;
  private final SchemaSettings schemaSettings;
  private final BatchSettings batchSettings;
  private final ExecutorSettings executorSettings;
  private final LogSettings logSettings;

  public SettingsManager(String[] args, String operationId) {
    config = parseUserSettings(args).withFallback(DEFAULT);
    config.checkValid(REFERENCE);
    // TODO check unrecognized config
    logSettings = new LogSettings(config.getConfig("log"), operationId);
    driverSettings = new DriverSettings(config.getConfig("driver"));
    connectorSettings = new ConnectorSettings(config.getConfig("connector"));
    schemaSettings = new SchemaSettings(config.getConfig("schema"));
    batchSettings = new BatchSettings(config.getConfig("batch"));
    executorSettings = new ExecutorSettings(config.getConfig("executor"));
  }

  public void logEffectiveSettings() {
    ConfigRenderOptions renderOptions =
        ConfigRenderOptions.defaults().setJson(false).setOriginComments(false).setComments(false);
    LOGGER.info("Loader effective settings:\n" + config.root().render(renderOptions));
  }

  public DriverSettings getDriverSettings() {
    return driverSettings;
  }

  public ConnectorSettings getConnectorSettings() {
    return connectorSettings;
  }

  public SchemaSettings getSchemaSettings() {
    return schemaSettings;
  }

  public BatchSettings getBatchSettings() {
    return batchSettings;
  }

  public ExecutorSettings getExecutorSettings() {
    return executorSettings;
  }

  public LogSettings getLogSettings() {
    return logSettings;
  }

  private static Config parseUserSettings(String[] args) {
    Config userSettings = ConfigFactory.empty();
    for (String arg : args) {
      userSettings = ConfigFactory.parseString(arg).withFallback(userSettings);
    }
    return userSettings;
  }
}
