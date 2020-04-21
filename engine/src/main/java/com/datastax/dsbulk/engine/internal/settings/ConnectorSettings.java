/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.engine.WorkflowType;
import com.typesafe.config.Config;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConnectorSettings {

  private final Config config;
  private final WorkflowType type;

  private Connector connector;
  private String connectorName;
  private Config connectorConfig;

  ConnectorSettings(Config config, WorkflowType type) {
    this.config = config;
    this.type = type;
  }

  public void init() {
    // Attempting to fetch the connector will run through all the validation logic we have for
    // parsing the configuration
    connectorName = config.getString("name");
    connector = locateConnector(connectorName);
    if (config.hasPath(connectorName)) {
      // the connector should be configured for reads when the workflow is LOAD
      boolean read = type == WorkflowType.LOAD;
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

  private static Connector locateConnector(String name) {
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
