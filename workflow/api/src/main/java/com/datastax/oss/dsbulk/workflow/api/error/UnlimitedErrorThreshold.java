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

public class UnlimitedErrorThreshold implements ErrorThreshold {

  public static final UnlimitedErrorThreshold INSTANCE = new UnlimitedErrorThreshold();

  private UnlimitedErrorThreshold() {}

  @Override
  public boolean checkThresholdExceeded(long errorCount, @NonNull Number totalItems) {
    return false;
  }

  @Override
  public String thresholdAsString() {
    return "unlimited";
  }
}
