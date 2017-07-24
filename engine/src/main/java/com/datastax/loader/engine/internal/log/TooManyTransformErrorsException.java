/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

/** */
public class TooManyTransformErrorsException extends RuntimeException {

  private final int maxTransformErrors;

  public TooManyTransformErrorsException(int maxTransformErrors) {
    super("Too many transform errors, the maximum allowed is " + maxTransformErrors);
    this.maxTransformErrors = maxTransformErrors;
  }

  public int getMaxTransformErrors() {
    return maxTransformErrors;
  }
}
