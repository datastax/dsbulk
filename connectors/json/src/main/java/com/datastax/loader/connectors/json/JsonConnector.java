/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.json;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.Connector;
import org.reactivestreams.Publisher;

/** */
public class JsonConnector implements Connector {

  @Override
  public Publisher<Statement> read() {
    return null;
  }

  @Override
  public void write(Publisher<Row> rows) {}
}
