/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * URLConnection implementation whose {@link #getOutputStream()} method always returns the {@link
 * System#out System standard output stream}.
 */
public class StandardOutputURLConnection extends URLConnection {

  public StandardOutputURLConnection(URL url) {
    super(url);
  }

  @Override
  public void connect() throws IOException {
    setDoOutput(true);
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return new BufferedOutputStream(new UncloseableOutputStream(System.out));
  }
}
