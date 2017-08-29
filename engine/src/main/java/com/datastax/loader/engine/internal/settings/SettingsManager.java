/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.commons.config.LoaderConfig;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SettingsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsManager.class);

  private final LoaderConfig config;
  private final String executionId;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;

  public SettingsManager(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void loadConfiguration() throws Exception {
    logSettings = new LogSettings(this.config.getConfig("log"), executionId);
    driverSettings = new DriverSettings(this.config.getConfig("driver"), executionId);
    connectorSettings = new ConnectorSettings(this.config.getConfig("connector"));
    schemaSettings = new SchemaSettings(this.config.getConfig("schema"));
    batchSettings = new BatchSettings(this.config.getConfig("batch"));
    executorSettings = new ExecutorSettings(this.config.getConfig("executor"));
    codecSettings = new CodecSettings(this.config.getConfig("codec"));
    monitoringSettings = new MonitoringSettings(this.config.getConfig("monitoring"), executionId);
  }

  public void logEffectiveSettings() {
    LOGGER.info("Loader effective settings:");
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      LOGGER.info(
          String.format(
              "%s = %s", entry.getKey(), entry.getValue().render(ConfigRenderOptions.concise())));
    }
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

  public CodecSettings getCodecSettings() {
    return codecSettings;
  }

  public MonitoringSettings getMonitoringSettings() {
    return monitoringSettings;
  }
}
