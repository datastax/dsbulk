/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.url;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * The main factory for URL stream handlers.
 *
 * <p>This class should be installed as the default URL stream handler factory at application
 * startup.
 *
 * <p>All non-standard URL schemes supported by DataStax loader should have an entry added here
 * forwarding to the appropriate URL stream handler.
 */
public class LoaderURLStreamHandlerFactory implements URLStreamHandlerFactory {

  /**
   * The scheme for standard input and standard output URLs. The only supported URL with such scheme
   * is {@code std:/}.
   */
  public static final String STD = "std";

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    if (STD.equalsIgnoreCase(protocol)) {
      return new StdinStdoutUrlStreamHandler();
    }
    // TODO other schemes: NFS, DSEFS...
    return null;
  }

  static class StdinStdoutUrlStreamHandler extends URLStreamHandler {
    StdinStdoutUrlStreamHandler() {}

    @Override
    protected URLConnection openConnection(URL url) {
      return new StdinStdoutConnection(url);
    }
  }

  private static class StdinStdoutConnection extends URLConnection {
    @Override
    public void connect() {}

    StdinStdoutConnection(URL url) {
      super(url);
    }

    @Override
    public InputStream getInputStream() {
      return new BufferedInputStream(new UncloseableInputStream(System.in));
    }

    @Override
    public OutputStream getOutputStream() {
      return new BufferedOutputStream(new UncloseablePrintStream(System.out));
    }
  }
}
