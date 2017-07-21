/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import io.reactivex.Flowable;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** */
public class CSVConnectorTest {

  @Test
  public void should_parse_csv() throws Exception {
    CSVConnector connector = new CSVConnector();
    Map<String, Object> settings = new HashMap<>();
    settings.put("url", CSVConnectorTest.class.getResource("/good.csv").toExternalForm());
    connector.configure(settings);
    Flowable.fromPublisher(connector.read()).blockingSubscribe();
  }

  @Test
  public void should_parse_bad_csv() throws Exception {
    CSVConnector connector = new CSVConnector();
    Map<String, Object> settings = new HashMap<>();
    settings.put("url", CSVConnectorTest.class.getResource("/bad.csv").toExternalForm());
    connector.configure(settings);
    Flowable.fromPublisher(connector.read()).blockingSubscribe();
  }
}
