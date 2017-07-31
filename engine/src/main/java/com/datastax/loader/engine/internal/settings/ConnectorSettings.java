/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.connectors.api.Connector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** */
public class ConnectorSettings {
  private final Connector connector;

  public ConnectorSettings(Config config) throws Exception {
    String connectorName = config.getString("name");
    connector = locateConnector(connectorName);
    ConfigObject connectorSettings = config.withoutPath("name").root();
    connector.configure(connectorSettings.unwrapped());
  }

  public Connector getConnector() {
    return connector;
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
        String.format("Cannot find connector '%s'; available connectors are: ", name)
            + StreamSupport.stream(connectors.spliterator(), false)
                .map(connector -> connector.getClass().getName())
                .collect(Collectors.joining(", ")));
  }
}
