/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.dsbulk.workflow.commons.utils.WorkflowUtils;
import com.typesafe.config.Config;

public class SettingsManager {

  private final Config config;

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

  public SettingsManager(Config config) {
    this.config = config;
  }

  public void init(String operationTitle, boolean configureConnectorForReads) {
    engineSettings = new EngineSettings(config.getConfig("dsbulk.engine"));
    engineSettings.init();
    String executionIdTemplate = engineSettings.getCustomExecutionIdTemplate();
    if (executionIdTemplate != null && !executionIdTemplate.isEmpty()) {
      this.executionId = WorkflowUtils.newCustomExecutionId(executionIdTemplate, operationTitle);
    } else {
      this.executionId = WorkflowUtils.newExecutionId(operationTitle);
    }
    logSettings = new LogSettings(config.getConfig("dsbulk.log"), this.executionId);
    driverSettings =
        new DriverSettings(
            config.getConfig("dsbulk.driver"),
            config.getConfig("dsbulk.executor.continuousPaging"),
            config.getConfig("datastax-java-driver"));
    connectorSettings =
        new ConnectorSettings(config.getConfig("dsbulk.connector"), configureConnectorForReads);
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
