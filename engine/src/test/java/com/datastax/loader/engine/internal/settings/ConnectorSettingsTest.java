/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.csv.CSVConnector;
import com.datastax.loader.connectors.json.JsonConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.datastax.loader.engine.internal.Assertions.assertThat;


public class ConnectorSettingsTest {
  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      ConfigFactory.defaultReference().getConfig("datastax-loader.connector");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void should_find_csv_connector_simple_name() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "name: csvConnector,  csvConnector: { url:\"file:///a/b.csv\"}"))
            .withFallback(replaceDefaultConnectorPathWithName("csvConnector"));
    ConnectorSettings connectorSettings = new ConnectorSettings(config);
    assertCSVConnectorSettings(connectorSettings, config, "csvConnector");
  }

  @Test
  public void should_find_csv_connector_short_name() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString("name: csv, csv{url:\"file:///a/b.csv\"}"))
            .withFallback(CONNECTOR_DEFAULT_SETTINGS);
    ConnectorSettings connectorSettings = new ConnectorSettings(config);
    assertCSVConnectorSettings(connectorSettings, config, "csv");
  }

  private static void assertCSVConnectorSettings(
      ConnectorSettings connectorSettings, LoaderConfig config, String connectorName) {
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(CSVConnector.class);
    assertThat(config.getConfig(connectorName))
        .hasPaths(
            "url",
            "pattern",
            "recursive",
            "maxThreads",
            "encoding",
            "header",
            "delimiter",
            "quote",
            "escape",
            "comment",
            "linesToSkip",
            "maxLines")
        .doesNotHavePath(connectorName);
  }

  @Test
  public void should_find_json_connector_full_path() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "name: com.datastax.loader.connectors.json.JsonConnector,  com.datastax.loader.connectors.json.JsonConnector{ url:\"file:///a/b.json\"}"));
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_find_json_connector_simple_name() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "name: jsonConnector, jsonConnector{ url:\"file:///a/b.json\"}"));
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_find_json_connector_short_name() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("name: json, json{ url:\"file:///a/b.json\"}"));
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_fail_for_nonexistent_connector() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("name: foo, foo {url:\"file:///a/b.txt\"}"));
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Cannot find connector 'foo'; available connectors are");
    new ConnectorSettings(config);
  }

  private Config replaceDefaultConnectorPathWithName(String name) {
    ConfigValue value = CONNECTOR_DEFAULT_SETTINGS.getValue("csv");
    return CONNECTOR_DEFAULT_SETTINGS.withValue(name, value);
  }
}
