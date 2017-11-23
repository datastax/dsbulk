/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

/** */
public class TooManyErrorsException extends RuntimeException {

  private final int maxErrors;

  public TooManyErrorsException(int maxErrors) {
    super("Too many errors, the maximum allowed is " + maxErrors);
    this.maxErrors = maxErrors;
  }

  public int getMaxErrors() {
    return maxErrors;
  }
}
