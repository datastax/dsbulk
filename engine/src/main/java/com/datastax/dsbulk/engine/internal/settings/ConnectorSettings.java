/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.DSBulkConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.engine.WorkflowType;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConnectorSettings {

  private final DSBulkConfig config;

  ConnectorSettings(DSBulkConfig config) throws Exception {
    this.config = config;
  }

  public Connector getConnector(WorkflowType workflowType) throws Exception {
    String connectorName = config.getString("name");
    Connector connector = locateConnector(connectorName);
    if (config.hasPath(connectorName)) {
      // the connector should be configured for reads when the workflow is LOAD
      boolean read = workflowType == WorkflowType.LOAD;
      connector.configure(config.getConfig(connectorName), read);
      return connector;
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot find configuration entry for connector '%s'", connectorName));
    }
  }

  private static Connector locateConnector(String name) {
    ServiceLoader<Connector> connectors = ServiceLoader.load(Connector.class);
    for (Connector connector : connectors) {
      // matches fully qualified class name
      if (connector.getClass().getName().equals(name)) return connector;
      // matches short names, i.e. "csv" will match "CSVConnector"
      if (connector.getClass().getSimpleName().toLowerCase().startsWith(name.toLowerCase()))
        return connector;
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
