/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

/** */
public class TooManyErrorsException extends RuntimeException {

  private final int maxLoadErrors;

  public TooManyErrorsException(int maxLoadErrors) {
    super("Too many errors, the maximum allowed is " + maxLoadErrors);
    this.maxLoadErrors = maxLoadErrors;
  }

  public int getMaxErrors() {
    return maxLoadErrors;
  }
}
