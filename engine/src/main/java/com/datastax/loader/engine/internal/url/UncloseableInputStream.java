/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.url;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that cannot be closed.
 *
 * <p>This is useful in rare situations where the underlying stream is being shared among consumers
 * and therefore should not be closed accidentally by one of them.
 */
public class UncloseableInputStream extends FilterInputStream {

  public UncloseableInputStream(InputStream in) {
    super(in);
  }

  @Override
  public void close() throws IOException {
    // do not forward the call to the delegate InputStream
  }
}
