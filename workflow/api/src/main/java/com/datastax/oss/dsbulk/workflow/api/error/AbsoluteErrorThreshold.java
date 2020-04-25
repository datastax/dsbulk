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

public class AbsoluteErrorThreshold implements ErrorThreshold {

  private final long maxErrors;

  AbsoluteErrorThreshold(long maxErrors) {
    if (maxErrors < 0) {
      throw new IllegalArgumentException("maxErrors must be >= 0");
    }
    this.maxErrors = maxErrors;
  }

  @Override
  public boolean checkThresholdExceeded(long errorCount, @NonNull Number totalItems) {
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
