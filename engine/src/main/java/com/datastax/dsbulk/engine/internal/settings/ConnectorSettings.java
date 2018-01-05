/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.engine.WorkflowType;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConnectorSettings {

  private final LoaderConfig config;
  private final WorkflowType type;

  private Connector connector;
  private String connectorName;
  private LoaderConfig connectorConfig;

  ConnectorSettings(LoaderConfig config, WorkflowType type) {
    this.config = config;
    this.type = type;
  }

  public void init() {
    // Attempting to fetch the connector will run through all the validation logic we have for parsing the configuration
    connectorName = config.getString("name");
    connector = locateConnector(connectorName);
    if (config.hasPath(connectorName)) {
      // the connector should be configured for reads when the workflow is LOAD
      boolean read = type == WorkflowType.LOAD;
      connectorConfig = config.getConfig(connectorName);
      connector.configure(connectorConfig, read);
    } else {
      throw new BulkConfigurationException(
          String.format("Cannot find configuration entry for connector '%s'", connectorName),
          "connector");
    }
  }

  public Connector getConnector() throws BulkConfigurationException {
    return connector;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public LoaderConfig getConnectorConfig() {
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
    throw new BulkConfigurationException(
        String.format(
            "Cannot find connector '%s'; available connectors are: %s",
            name,
            StreamSupport.stream(connectors.spliterator(), false)
                .map(connector -> connector.getClass().getName())
                .collect(Collectors.joining(", "))),
        "connector");
  }
}
