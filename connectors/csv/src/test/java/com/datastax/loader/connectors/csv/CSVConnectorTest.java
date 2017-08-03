/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.internal.MapRecord;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.reactivex.Flowable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.List;
import org.junit.Test;

/** */
public class CSVConnectorTest {

  @Test
  public void should_read_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        ConfigFactory.parseString(
            String.format(
                "header = true, url = \"%s\", escape = \"\\\"\", comment = \"#\"",
                url("/sample.csv")));
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
    URL.setURLStreamHandlerFactory(
        protocol -> {
          if (protocol.equalsIgnoreCase("stdin")) {
            return new URLStreamHandler() {
              @Override
              protected URLConnection openConnection(URL u) throws IOException {
                return new URLConnection(u) {
                  @Override
                  public void connect() throws IOException {}

                  @Override
                  public InputStream getInputStream() throws IOException {
                    return System.in;
                  }
                };
              }
            };
          }
          return null;
        });
    InputStream stdin = System.in;
    try {
      String line = "fóô,bàr,qïx\n";
      InputStream is = new ByteArrayInputStream(line.getBytes("ISO-8859-1"));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      Config settings = ConfigFactory.parseString("url = \"stdin:/\", encoding = ISO-8859-1");
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
    Config settings =
        ConfigFactory.parseString(
            String.format("header = true, url = \"%s\", recursive = false", url("/root")));
    connector.configure(settings);
    connector.init();
    assertThat(Flowable.fromPublisher(connector.read()).count().blockingGet()).isEqualTo(300);
  }

  @Test
  public void should_scan_directory_recursively() throws Exception {
    CSVConnector connector = new CSVConnector();
    Config settings =
        ConfigFactory.parseString(
            String.format("header = true, url = \"%s\", recursive = true", url("/root")));
    connector.configure(settings);
    connector.init();
    assertThat(Flowable.fromPublisher(connector.read()).count().blockingGet()).isEqualTo(500);
  }

  private static String url(String resource) {
    return CSVConnectorTest.class.getResource(resource).toExternalForm();
  }
}
