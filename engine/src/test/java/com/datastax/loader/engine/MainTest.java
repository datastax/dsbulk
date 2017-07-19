/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import org.junit.Ignore;
import org.junit.Test;

/** */
public class MainTest {

  // TODO temporary, remove when end-to-end integration tests are available
  @Test
  @Ignore
  public void should_load() {

    String[] args = {
      "connector.class=com.datastax.loader.connectors.csv.CSVConnector",
      "driver.query.consistency=ONE",
      "schema.mapping={0=c2,2=c1}"
    };

    new Main(args).load();
  }
}
