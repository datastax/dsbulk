/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.loader.engine.internal.Assertions.assertThat;

import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.csv.CSVConnector;
import com.datastax.loader.connectors.json.JsonConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** */
public class ConnectorSettingsTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void should_find_csv_connector_full_path() throws Exception {
    Config config =
        ConfigFactory.parseString(
            "name: com.datastax.loader.connectors.csv.CSVConnector, url:\"file:///a/b.csv\"");
    ConnectorSettings connectorSettings = new ConnectorSettings(config);
    assertCSVConnectorSettings(connectorSettings);
  }

  @Test
  public void should_find_csv_connector_simple_name() throws Exception {
    Config config = ConfigFactory.parseString("name: csvConnector, url:\"file:///a/b.csv\"");
    ConnectorSettings connectorSettings = new ConnectorSettings(config);
    assertCSVConnectorSettings(connectorSettings);
  }

  @Test
  public void should_find_csv_connector_short_name() throws Exception {
    Config config = ConfigFactory.parseString("name: csv, url:\"file:///a/b.csv\"");
    ConnectorSettings connectorSettings = new ConnectorSettings(config);
    assertCSVConnectorSettings(connectorSettings);
  }

  private static void assertCSVConnectorSettings(ConnectorSettings connectorSettings) {
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(CSVConnector.class);
    assertThat(connectorSettings.getConnectorEffectiveSettings())
        .hasPaths(
            "name",
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
        .doesNotHavePath("csv");
  }

  @Test
  public void should_find_json_connector_full_path() throws Exception {
    Config config =
        ConfigFactory.parseString(
            "name: com.datastax.loader.connectors.json.JsonConnector, url:\"file:///a/b.json\"");
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_find_json_connector_simple_name() throws Exception {
    Config config = ConfigFactory.parseString("name: jsonConnector, url:\"file:///a/b.json\"");
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_find_json_connector_short_name() throws Exception {
    Config config = ConfigFactory.parseString("name: json, url:\"file:///a/b.json\"");
    Connector connector = new ConnectorSettings(config).getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
  }

  @Test
  public void should_fail_for_nonexistent_connector() throws Exception {
    Config config = ConfigFactory.parseString("name: foo, url:\"file:///a/b.txt\"");
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Cannot find connector 'foo'; available connectors are");
    new ConnectorSettings(config);
  }
}
