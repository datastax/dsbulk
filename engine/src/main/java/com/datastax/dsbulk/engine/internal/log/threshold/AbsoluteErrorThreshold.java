/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.threshold;

import org.jetbrains.annotations.NotNull;

public class AbsoluteErrorThreshold implements ErrorThreshold {

  private final long maxErrors;

  AbsoluteErrorThreshold(long maxErrors) {
    if (maxErrors < 0) {
      throw new IllegalArgumentException("maxErrors must be >= 0");
    }
    this.maxErrors = maxErrors;
  }

  @Override
  public boolean checkThresholdExceeded(long errorCount, @NotNull Number totalItems) {
    return errorCount > maxErrors;
  }

  @Override
  public String thresholdAsString() {
    return Long.toString(maxErrors);
  }

  public long getMaxErrors() {
    return maxErrors;
  }
}
