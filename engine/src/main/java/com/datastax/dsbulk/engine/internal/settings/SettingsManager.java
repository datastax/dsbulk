/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.typesafe.config.Config;

public class SettingsManager {

  private final Config config;
  private final BiMap<String, String> shortcuts;
  private final WorkflowType workflowType;

  private String executionId;

  private DriverSettings driverSettings;
  private ConnectorSettings connectorSettings;
  private SchemaSettings schemaSettings;
  private BatchSettings batchSettings;
  private ExecutorSettings executorSettings;
  private LogSettings logSettings;
  private CodecSettings codecSettings;
  private MonitoringSettings monitoringSettings;
  private EngineSettings engineSettings;
  private StatsSettings statsSettings;

  public SettingsManager(
      Config config, BiMap<String, String> shortcuts, WorkflowType workflowType) {
    this.config = config;
    this.shortcuts = shortcuts;
    this.workflowType = workflowType;
  }

  public void init() {
    engineSettings = new EngineSettings(config.getConfig("dsbulk.engine"));
    engineSettings.init();
    String executionIdTemplate = engineSettings.getCustomExecutionIdTemplate();
    if (executionIdTemplate != null && !executionIdTemplate.isEmpty()) {
      this.executionId = WorkflowUtils.newCustomExecutionId(executionIdTemplate, workflowType);
    } else {
      this.executionId = WorkflowUtils.newExecutionId(workflowType);
    }
    logSettings = new LogSettings(config.getConfig("dsbulk.log"), this.executionId);
    driverSettings =
        new DriverSettings(
            config.getConfig("dsbulk.driver"),
            config.getConfig("dsbulk.executor.continuousPaging"),
            config.getConfig("datastax-java-driver"),
            shortcuts);
    connectorSettings = new ConnectorSettings(config.getConfig("dsbulk.connector"), workflowType);
    batchSettings = new BatchSettings(config.getConfig("dsbulk.batch"));
    executorSettings = new ExecutorSettings(config.getConfig("dsbulk.executor"));
    codecSettings = new CodecSettings(config.getConfig("dsbulk.codec"));
    schemaSettings = new SchemaSettings(config.getConfig("dsbulk.schema"));
    monitoringSettings =
        new MonitoringSettings(config.getConfig("dsbulk.monitoring"), this.executionId);
    statsSettings = new StatsSettings(config.getConfig("dsbulk.stats"));
  }

  public String getExecutionId() {
    return executionId;
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

  public StatsSettings getStatsSettings() {
    return statsSettings;
  }

  public Config getBulkLoaderConfig() {
    // must be called after connector settings initialized
    Config dsbulkConfig =
        config
            .getConfig("dsbulk")
            .withoutPath("metaSettings")
            // limit connector configuration to the selected connector
            .withoutPath("connector");
    if (connectorSettings.getConnectorConfig() != null) {
      dsbulkConfig =
          dsbulkConfig
              .withFallback(
                  connectorSettings
                      .getConnectorConfig()
                      .atPath("connector." + connectorSettings.getConnectorName()))
              .withoutPath("config");
    }
    return dsbulkConfig;
  }
}
