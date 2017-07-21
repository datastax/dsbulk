/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

/** */
public class TooManyLoadErrorsException extends RuntimeException {

  private final int maxLoadErrors;

  public TooManyLoadErrorsException(int maxLoadErrors) {
    super("Too many load errors, the maximum allowed is " + maxLoadErrors);
    this.maxLoadErrors = maxLoadErrors;
  }

  public int getMaxLoadErrors() {
    return maxLoadErrors;
  }
}
