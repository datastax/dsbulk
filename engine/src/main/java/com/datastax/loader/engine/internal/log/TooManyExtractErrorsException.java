/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

/** */
public class TooManyExtractErrorsException extends RuntimeException {

  private final int maxExtractErrors;

  public TooManyExtractErrorsException(int maxExtractErrors) {
    super("Too many extract errors, the maximum allowed is " + maxExtractErrors);
    this.maxExtractErrors = maxExtractErrors;
  }

  public int getMaxExtractErrors() {
    return maxExtractErrors;
  }
}
