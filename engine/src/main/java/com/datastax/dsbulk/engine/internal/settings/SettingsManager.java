/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SettingsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsManager.class);

  private final LoaderConfig config;
  private final String executionId;
  private final WorkflowType workflowType;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;
  private EngineSettings engineSettings;

  public SettingsManager(LoaderConfig config, String executionId, WorkflowType workflowType) {
    this.config = config;
    this.executionId = executionId;
    this.workflowType = workflowType;
  }

  public void loadConfiguration() throws BulkConfigurationException {
    logSettings = new LogSettings(config.getConfig("log"), executionId);
    driverSettings = new DriverSettings(config.getConfig("driver"), executionId);
    connectorSettings = new ConnectorSettings(config.getConfig("connector"), workflowType);
    batchSettings = new BatchSettings(config.getConfig("batch"));
    executorSettings = new ExecutorSettings(config.getConfig("executor"));
    codecSettings = new CodecSettings(config.getConfig("codec"));
    schemaSettings =
        new SchemaSettings(config.getConfig("schema"), codecSettings.getTimestampCodec());
    monitoringSettings = new MonitoringSettings(config.getConfig("monitoring"), executionId);
    engineSettings = new EngineSettings(config.getConfig("engine"));
  }

  public void logEffectiveSettings() {
    LOGGER.info("Bulk Loader effective settings:");

    Set<Map.Entry<String, ConfigValue>> entries =
        new TreeSet<>(Comparator.comparing(Map.Entry::getKey));
    entries.addAll(config.entrySet());

    for (Map.Entry<String, ConfigValue> entry : entries) {
      LOGGER.info(
          String.format(
              "%s = %s", entry.getKey(), entry.getValue().render(ConfigRenderOptions.concise())));
    }
    LOGGER.info("Available CPU cores: " + Runtime.getRuntime().availableProcessors());
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

  public EngineSettings getEngineSettings() {
    return engineSettings;
  }
}
