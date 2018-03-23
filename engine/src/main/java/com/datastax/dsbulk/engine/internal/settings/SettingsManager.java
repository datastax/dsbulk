/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SettingsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SettingsManager.class);

  private final LoaderConfig config;
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

  public SettingsManager(LoaderConfig config, WorkflowType workflowType) {
    this.config = config;
    this.workflowType = workflowType;
  }

  public void init() {
    engineSettings = new EngineSettings(config.getConfig("engine"));
    engineSettings.init();
    String executionIdTemplate = engineSettings.getCustomExecutionIdTemplate();
    if (executionIdTemplate != null && !executionIdTemplate.isEmpty()) {
      this.executionId = WorkflowUtils.newCustomExecutionId(executionIdTemplate, workflowType);
    } else {
      this.executionId = WorkflowUtils.newExecutionId(workflowType);
    }
    logSettings = new LogSettings(config.getConfig("log"), this.executionId);
    driverSettings = new DriverSettings(config.getConfig("driver"), this.executionId);
    connectorSettings = new ConnectorSettings(config.getConfig("connector"), workflowType);
    batchSettings = new BatchSettings(config.getConfig("batch"));
    executorSettings = new ExecutorSettings(config.getConfig("executor"));
    codecSettings = new CodecSettings(config.getConfig("codec"));
    schemaSettings = new SchemaSettings(config.getConfig("schema"));
    monitoringSettings = new MonitoringSettings(config.getConfig("monitoring"), this.executionId);
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
}
