/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.url;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * An input stream that cannot be closed.
 *
 * <p>This is useful in rare situations where the underlying stream is being shared among consumers
 * and therefore should not be closed accidentally by one of them.
 */
@SuppressWarnings("WeakerAccess")
public class UncloseableInputStream extends FilterInputStream {

  public UncloseableInputStream(InputStream in) {
    super(in);
  }

  @Override
  public void close() {
    // do not forward the call to the delegate InputStream
  }
}
