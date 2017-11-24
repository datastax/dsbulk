/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * URLConnection implementation whose {@link #getInputStream()} method always returns the {@link
 * System#in System standard input stream}.
 */
public class StandardInputURLConnection extends URLConnection {

  public StandardInputURLConnection(URL url) {
    super(url);
  }

  @Override
  public void connect() throws IOException {}

  @Override
  public InputStream getInputStream() throws IOException {
    return new BufferedInputStream(new UncloseableInputStream(System.in));
  }
}
