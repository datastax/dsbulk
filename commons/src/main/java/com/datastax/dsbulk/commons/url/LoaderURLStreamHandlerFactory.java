/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.url;

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

  /** The scheme for standard input URLs. The only supported URL with such scheme is "stdin:/". */
  private static final String STDIN = "stdin";

  /** The scheme for standard output URLs. The only supported URL with such scheme is "stdout:/". */
  private static final String STDOUT = "stdout";

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    if (STDIN.equalsIgnoreCase(protocol)) {
      return new StandardInputURLStreamHandler();
    }
    if (STDOUT.equalsIgnoreCase(protocol)) {
      return new StandardOutputURLStreamHandler();
    }
    // TODO other schemes: NFS, DSEFS...
    return null;
  }
}
