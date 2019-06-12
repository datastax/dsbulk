/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.threshold;

import java.util.concurrent.atomic.LongAdder;

public class RatioErrorThreshold implements ErrorThreshold {

  private final float maxErrorRatio;
  private final long minSample;

  RatioErrorThreshold(float maxErrorRatio, long minSample) {
    if (maxErrorRatio <= 0 || maxErrorRatio >= 1) {
      throw new IllegalArgumentException("maxErrorRatio must be > 0 and < 1");
    }
    if (minSample < 1) {
      throw new IllegalArgumentException("minSample must be >= 1");
    }
    this.maxErrorRatio = maxErrorRatio;
    this.minSample = minSample;
  }

  @Override
  public boolean checkThresholdExceeded(long errorCount, LongAdder totalItems) {
    long totalSoFar = totalItems.sum();
    if (totalSoFar >= minSample) {
      float currentRatio = (float) errorCount / totalSoFar;
      return currentRatio > maxErrorRatio;
    }
    return false;
  }

  public float getMaxErrorRatio() {
    return maxErrorRatio;
  }

  public long getMinSample() {
    return minSample;
  }
}
