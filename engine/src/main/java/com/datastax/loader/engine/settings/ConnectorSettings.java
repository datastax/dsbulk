/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.engine.internal.ReflectionUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

/** */
public class ConnectorSettings {

  private final Config config;

  public ConnectorSettings(Config config) {
    this.config = config;
  }

  public Connector newConnector() {
    Connector connector = ReflectionUtils.newInstance(config.getString("class"));
    ConfigObject connectorSettings = config.withoutPath("class").root();
    connector.configure(connectorSettings.unwrapped());
    return connector;
  }
}
