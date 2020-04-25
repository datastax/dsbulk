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

public interface ErrorThreshold {

  /** Creates an {@link ErrorThreshold} based on an absolute number of maximum allowed errors. */
  static ErrorThreshold forAbsoluteValue(long maxErrors) {
    return new AbsoluteErrorThreshold(maxErrors);
  }

  /**
   * Creates an {@link ErrorThreshold} based on a ratio of errors over the total items processed so
   * far.
   *
   * @param maxErrorRatio the maximum error ratio to tolerate, must be &gt; 0 and &lt; 1.
   * @param minSample the minimum sample count to observe; as long as the total number of items
   *     processed is lesser than this number, the error ratio will not be tested.
   */
  static ErrorThreshold forRatio(float maxErrorRatio, long minSample) {
    return new RatioErrorThreshold(maxErrorRatio, minSample);
  }

  /** Creates an {@link ErrorThreshold} that cannot be exceeded. */
  static ErrorThreshold unlimited() {
    return UnlimitedErrorThreshold.INSTANCE;
  }

  /**
   * Checks whether the error threshold was exceeded.
   *
   * @param errorCount the current number of errors encountered.
   * @param totalItems the total number of items processed so far.
   * @return {@code true} if the threshold was exceeded, or {@code false} otherwise.
   */
  boolean checkThresholdExceeded(long errorCount, @NonNull Number totalItems);

  /**
   * Returns a textual description of this threshold, mainly for informational purposes, e.g. when
   * creating error messages.
   *
   * @return textual description of this threshold.
   */
  String thresholdAsString();
}
