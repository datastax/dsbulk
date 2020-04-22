/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
