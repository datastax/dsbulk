/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.dist;

import com.datastax.loader.connectors.api.Connector;
import com.typesafe.config.Config;

/** */
public class ConnectorFactory {

  public ConnectorFactory(String connectorClass, Config connectorsConfig) {}

  public Connector newInstance() {
    // TODO
    return null;
  }
}
