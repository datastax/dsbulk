/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
