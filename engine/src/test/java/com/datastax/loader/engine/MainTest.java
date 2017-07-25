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
  public void should_load() throws Exception {

    /*
    create keyspace ks with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    create table ks.t1 (Year int primary key,Make varchar,Model varchar,Description varchar,Price decimal);
     */
    String[] args = {
      "log.output-directory=\"file:./target\"",
      "connector.class=com.datastax.loader.connectors.csv.CSVConnector",
      "connector.url=\"" + MainTest.class.getResource("/bad.csv").toExternalForm() + "\"",
      "driver.query.consistency=ONE",
      "schema.keyspace=ks",
      "schema.table=t1",
      "schema.mapping={0=year,1=make,2=model,3=description,4=price}"
    };

    new Main(args).load();
  }
}
