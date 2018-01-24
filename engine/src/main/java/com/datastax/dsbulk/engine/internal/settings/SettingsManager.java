/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.settings.LogSettings.MAIN_LOG_FILE_APPENDER;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.PRODUCTION_KEY;

import ch.qos.logback.classic.LoggerContext;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.utils.HelpUtils;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.typesafe.config.Config;
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

  public void logEffectiveSettings(String connectorName, Config connectorConfig) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(HelpUtils.getVersionMessage() + " starting.");
      ch.qos.logback.classic.Logger root =
          (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      LoggerContext lc = root.getLoggerContext();
      String production = lc.getProperty(PRODUCTION_KEY);
      if (production != null && production.equalsIgnoreCase("true")) {
        ch.qos.logback.classic.Logger effectiveSettingsLogger =
            (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(
                    "com.datastax.dsbulk.engine.internal.settings.EFFECTIVE_SETTINGS");
        effectiveSettingsLogger.setAdditive(false);
        effectiveSettingsLogger.addAppender(root.getAppender(MAIN_LOG_FILE_APPENDER));
        effectiveSettingsLogger.info("Effective settings:");
        Set<Map.Entry<String, ConfigValue>> entries =
            new TreeSet<>(Comparator.comparing(Map.Entry::getKey));
        entries.addAll(
            config
                .withoutPath("metaSettings")
                // limit connector configuration to the selected connector
                .withoutPath("connector")
                .withFallback(connectorConfig.atPath("connector." + connectorName))
                .entrySet());
        for (Map.Entry<String, ConfigValue> entry : entries) {
          // Skip all settings that have a `metaSettings` path element.
          if (entry.getKey().contains(".metaSettings.")) {
            continue;
          }
          effectiveSettingsLogger.info(
              String.format(
                  "%s = %s",
                  entry.getKey(), entry.getValue().render(ConfigRenderOptions.concise())));
        }
      }
      LOGGER.info("Available CPU cores: {}.", Runtime.getRuntime().availableProcessors());
      LOGGER.info("Operation output directory: {}.", logSettings.getExecutionDirectory());
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

  public EngineSettings getEngineSettings() {
    return engineSettings;
  }
}
