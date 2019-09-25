/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import com.datastax.dsbulk.engine.internal.log.threshold.ErrorThreshold;

/**
 * Thrown when the engine encounters too many errors. This exception triggers the operation
 * abortion.
 */
public class TooManyErrorsException extends RuntimeException {

  private final ErrorThreshold threshold;

  public TooManyErrorsException(ErrorThreshold threshold) {
    super(createErrorMessage(threshold));
    this.threshold = threshold;
  }

  private static String createErrorMessage(ErrorThreshold threshold) {
    return "Too many errors, the maximum allowed is " + threshold.thresholdAsString() + ".";
  }

  public ErrorThreshold getThreshold() {
    return threshold;
  }
}
