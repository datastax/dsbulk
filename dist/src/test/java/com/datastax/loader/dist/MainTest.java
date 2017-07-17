/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.dist;

import org.junit.Ignore;
import org.junit.Test;

/** */
public class MainTest {

  @Ignore
  @Test
  public void should_load() {

    String[] args = {
      "connector-class=com.datastax.loader.connectors.csv.CSVConnector",
      "connection.query-options.consistency-level=ONE"
    };

    new Main(args).load();
  }
}
