/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.internal.MapRecord;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.reactivex.Flowable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import org.junit.Test;

/** */
public class CSVConnectorTest {
  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      ConfigFactory.defaultReference().getConfig("datastax-loader.connector");

  @Test
  public void should_read_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "header = true, url = \"%s\", escape = \"\\\"\", comment = \"#\"",
                        url("/sample.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings);
    connector.init();
    List<Record> actual = Flowable.fromPublisher(connector.read()).toList().blockingGet();
    assertThat(actual).hasSize(5);
    assertThat(((MapRecord) actual.get(0)).values())
        .containsOnly("1997", "Ford", "E350", "ac, abs, moon", "3000.00");
    assertThat(((MapRecord) actual.get(1)).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition\"", null, "4900.00");
    assertThat(((MapRecord) actual.get(2)).values())
        .containsOnly(
            "1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00");
    assertThat(((MapRecord) actual.get(3)).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", null, "5000.00");
    assertThat(((MapRecord) actual.get(4)).values())
        .containsOnly(null, null, "Venture \"Extended Edition\"", null, "4900.00");
  }

  @Test
  public void should_read_from_stdin_with_special_encoding() throws Exception {
    URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    InputStream stdin = System.in;
    try {
      String line = "fóô,bàr,qïx\n";
      InputStream is = new ByteArrayInputStream(line.getBytes("ISO-8859-1"));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("url = \"stdin:/\", encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings);
      connector.init();
      List<Record> actual = Flowable.fromPublisher(connector.read()).toList().blockingGet();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(line);
      assertThat(((MapRecord) actual.get(0)).values()).containsExactly("fóô", "bàr", "qïx");
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  public void should_scan_directory() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("header = true, url = \"%s\", recursive = false", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings);
    connector.init();
    assertThat(Flowable.fromPublisher(connector.read()).count().blockingGet()).isEqualTo(300);
  }

  @Test
  public void should_scan_directory_with_path() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "header = true, url = \"%s\", recursive = false",
                        CSVConnectorTest.class.getResource("/root").toExternalForm()))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings);
    connector.init();
    assertThat(Flowable.fromPublisher(connector.read()).count().blockingGet()).isEqualTo(300);
  }

  @Test
  public void should_scan_directory_recursively() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("header = true, url = \"%s\", recursive = true", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings);
    connector.init();
    assertThat(Flowable.fromPublisher(connector.read()).count().blockingGet()).isEqualTo(500);
  }

  private static String url(String resource) {
    return CSVConnectorTest.class.getResource(resource).toExternalForm();
  }
}
