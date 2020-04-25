/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.api.error;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.text.DecimalFormat;

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
  public boolean checkThresholdExceeded(long errorCount, @NonNull Number totalItems) {
    long totalSoFar = totalItems.longValue();
    if (totalSoFar >= minSample) {
      float currentRatio = (float) errorCount / totalSoFar;
      return currentRatio > maxErrorRatio;
    }
    return false;
  }

  @Override
  public String thresholdAsString() {
    return new DecimalFormat("#.##%").format(maxErrorRatio);
  }

  public float getMaxErrorRatio() {
    return maxErrorRatio;
  }

  public long getMinSample() {
    return minSample;
  }
}
