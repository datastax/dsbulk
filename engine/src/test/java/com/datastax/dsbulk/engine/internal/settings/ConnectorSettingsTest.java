/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.csv.CSVConnector;
import com.datastax.dsbulk.connectors.json.JsonConnector;
import com.datastax.dsbulk.engine.WorkflowType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.junit.jupiter.api.Test;

class ConnectorSettingsTest {
  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      ConfigFactory.defaultReference().getConfig("dsbulk.connector");

  @Test
  void should_find_csv_connector_simple_name() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "name: csvConnector,  csvConnector: { url:\"file:///a/b.csv\"}"))
            .withFallback(replaceDefaultConnectorPathWithName("csvConnector"));
    ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
    connectorSettings.init();
    assertCSVConnectorSettings(connectorSettings, config, "csvConnector");
  }

  @Test
  void should_find_csv_connector_short_name() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString("name: csv, csv{url:\"file:///a/b.csv\"}"))
            .withFallback(CONNECTOR_DEFAULT_SETTINGS);
    ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
    connectorSettings.init();
    assertCSVConnectorSettings(connectorSettings, config, "csv");
  }

  @Test
  void should_find_json_connector_full_path() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "name: com.datastax.dsbulk.connectors.json.JsonConnector,  com.datastax.dsbulk.connectors.json.JsonConnector{ url:\"file:///a/b.json\"}"));
    ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
    connectorSettings.init();
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  void should_find_json_connector_simple_name() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "name: jsonConnector, jsonConnector{ url:\"file:///a/b.json\"}"));
    ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
    connectorSettings.init();
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  void should_find_json_connector_short_name() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("name: json, json{ url:\"file:///a/b.json\"}"));
    ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
    connectorSettings.init();
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  void should_fail_for_nonexistent_connector() {
    assertThrows(
        BulkConfigurationException.class,
        () -> {
          LoaderConfig config =
              new DefaultLoaderConfig(
                  ConfigFactory.parseString("name: foo, foo {url:\"file:///a/b.txt\"}"));
          ConnectorSettings connectorSettings = new ConnectorSettings(config, WorkflowType.LOAD);
          connectorSettings.init();
          //noinspection ResultOfMethodCallIgnored
          connectorSettings.getConnector();
        },
        "Cannot find connector 'foo'; available connectors are");
  }

  private Config replaceDefaultConnectorPathWithName(
      @SuppressWarnings("SameParameterValue") String name) {
    ConfigValue value = CONNECTOR_DEFAULT_SETTINGS.getValue("csv");
    return CONNECTOR_DEFAULT_SETTINGS.withValue(name, value);
  }

  private static void assertCSVConnectorSettings(
      ConnectorSettings connectorSettings, LoaderConfig config, String connectorName) {
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(CSVConnector.class);
    assertThat(config.getConfig(connectorName))
        .hasPaths(
            "url",
            "fileNamePattern",
            "recursive",
            "maxConcurrentFiles",
            "encoding",
            "header",
            "delimiter",
            "quote",
            "escape",
            "comment",
            "skipLines",
            "maxLines")
        .doesNotHavePath(connectorName);
  }
}
