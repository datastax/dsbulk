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
package com.datastax.oss.dsbulk.tests.driver;

import com.datastax.oss.driver.api.core.Version;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class VersionUtils {

  /**
   * Whether {@code version} is within the range defined by {@code min} and {@code max}.
   *
   * <p>The lower bound is inclusive; the upper bound is exclusive. Both bounds are allowed to be
   * null, in which case that bound is considered unbounded.
   *
   * @param version The version to check.
   * @param min The lower inclusive bound, or null the lower bound is unbounded.
   * @param max The upper exclusive bound, or null the upper bound is unbounded.
   * @return true if the version is within the range, and false otherwise.
   */
  public static boolean isWithinRange(
      @NonNull Version version, @Nullable Version min, @Nullable Version max) {
    return ((min == null) && (max == null))
        || (((min == null) || (min.compareTo(version) <= 0))
            && ((max == null) || (max.compareTo(version) > 0)));
  }
}
