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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.connectors.json.JsonConnector;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class ConnectorSettingsTest {

  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      TestConfigUtils.createTestConfig("dsbulk.connector");

  @Test
  void should_find_csv_connector_short_name() {
    Config config =
        ConfigFactory.parseString("name: csv, csv{url:\"file:///a/b.csv\"}")
            .withFallback(CONNECTOR_DEFAULT_SETTINGS);
    ConnectorSettings connectorSettings = new ConnectorSettings(config, true);
    connectorSettings.init(true);
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(CSVConnector.class);
    assertThat(config.getConfig("csv"))
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
            "skipRecords",
            "maxRecords")
        .doesNotHavePath("csv");
  }

  @Test
  void should_find_json_connector_short_name() {
    Config config =
        ConfigFactory.parseString("name: json, json{ url:\"file:///a/b.json\"}")
            .withFallback(CONNECTOR_DEFAULT_SETTINGS);
    ConnectorSettings connectorSettings = new ConnectorSettings(config, true);
    connectorSettings.init(true);
    Connector connector = connectorSettings.getConnector();
    assertThat(connector).isNotNull().isInstanceOf(JsonConnector.class);
    assertThat(config.getConfig("json"))
        .hasPaths(
            "url",
            "fileNamePattern",
            "recursive",
            "maxConcurrentFiles",
            "encoding",
            "skipRecords",
            "maxRecords",
            "parserFeatures",
            "generatorFeatures",
            "prettyPrint")
        .doesNotHavePath("json");
  }

  @Test
  void should_fail_for_nonexistent_connector() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Config config = ConfigFactory.parseString("name: foo, foo {url:\"file:///a/b.txt\"}");
          ConnectorSettings connectorSettings = new ConnectorSettings(config, true);
          connectorSettings.init(true);
          //noinspection ResultOfMethodCallIgnored
          connectorSettings.getConnector();
        },
        "Cannot find connector 'foo'; available connectors are");
  }
}
