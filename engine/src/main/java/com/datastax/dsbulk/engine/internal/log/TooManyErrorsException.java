/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

/**
 * Thrown when the engine encounters too many errors. This exception triggers the operation
 * abortion.
 */
public class TooManyErrorsException extends RuntimeException {

  private final int maxErrors;
  private final float maxErrorRatio;

  TooManyErrorsException(int maxErrors) {
    super("Too many errors, the maximum allowed is " + maxErrors);
    this.maxErrors = maxErrors;
    maxErrorRatio = -1;
  }

  TooManyErrorsException(float maxErrorRatio) {
    super("Too many errors, the maximum percentage allowed is " + (maxErrorRatio * 100f) + "%");
    this.maxErrorRatio = maxErrorRatio;
    maxErrors = -1;
  }

  public int getMaxErrors() {
    return maxErrors;
  }

  public float getMaxErrorRatio() {
    return maxErrorRatio;
  }
}
