/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.url;

import java.io.IOException;
import java.net.URL;
import java.net.URLStreamHandler;

/** URL stream handler for {@link System#in standard input}. */
public class StandardInputURLStreamHandler extends URLStreamHandler {

  @Override
  public StandardInputURLConnection openConnection(URL url) throws IOException {
    return new StandardInputURLConnection(url);
  }
}
