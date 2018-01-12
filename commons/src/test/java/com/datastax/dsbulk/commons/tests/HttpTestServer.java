/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.tests;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.form.EagerFormParsingHandler;
import io.undertow.server.handlers.form.FormParserFactory;
import io.undertow.server.handlers.form.MultiPartParserDefinition;
import java.io.IOException;
import java.net.ServerSocket;

/** */
public class HttpTestServer {

  private int port;
  private Undertow server;

  public void start(HttpHandler handler) throws IOException {
    port = findFreePort();
    server =
        Undertow.builder()
            .addHttpListener(port, "localhost")
            .setHandler(
                new EagerFormParsingHandler(
                        FormParserFactory.builder()
                            .addParsers(new MultiPartParserDefinition())
                            .build())
                    .setNext(handler))
            .build();
    server.start();
  }

  public void stop() {
    server.stop();
  }

  public int getPort() {
    return port;
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
