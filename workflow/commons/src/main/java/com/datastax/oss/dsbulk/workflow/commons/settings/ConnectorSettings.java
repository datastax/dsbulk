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

import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.typesafe.config.Config;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConnectorSettings {

  private final Config config;
  private final boolean read;

  private Connector connector;
  private String connectorName;
  private Config connectorConfig;

  public ConnectorSettings(Config config, boolean read) {
    this.config = config;
    this.read = read;
  }

  public void init() {
    // Attempting to fetch the connector will run through all the validation logic we have for
    // parsing the configuration
    connectorName = config.getString("name");
    connector = locateConnector(connectorName);
    if (config.hasPath(connectorName)) {
      // the connector should be configured for reads when the workflow is LOAD
      connectorConfig = config.getConfig(connectorName).withoutPath("metaSettings");
      connector.configure(connectorConfig, read);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot find configuration entry for connector '%s'", connectorName));
    }
  }

  public Connector getConnector() throws IllegalArgumentException {
    return connector;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public Config getConnectorConfig() {
    return connectorConfig;
  }

  protected Connector locateConnector(String name) {
    ServiceLoader<Connector> connectors = ServiceLoader.load(Connector.class);
    for (Connector connector : connectors) {
      // matches fully qualified class name
      if (connector.getClass().getName().equals(name)) {
        return connector;
      }
      // matches short names, i.e. "csv" will match "CSVConnector"
      if (connector.getClass().getSimpleName().toLowerCase().startsWith(name.toLowerCase())) {
        return connector;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Cannot find connector '%s'; available connectors are: %s",
            name,
            StreamSupport.stream(connectors.spliterator(), false)
                .map(connector -> connector.getClass().getName())
                .collect(Collectors.joining(", "))));
  }
}
