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
package com.datastax.oss.dsbulk.tests.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a simplified version of the driver's VersionNumber class, to avoid references to the
 * driver jar.
 *
 * <p>It can only parse x.y.z.w version numbers.
 */
public final class Version implements Comparable<Version> {

  private static final Pattern PATTERN = Pattern.compile("(\\d+)\\.(\\d+)(\\.(\\d+))?(\\.(\\d+))?");

  public static Version parse(String versionStr) {
    if (versionStr == null || versionStr.isEmpty()) {
      return null;
    }
    Matcher matcher = PATTERN.matcher(versionStr);
    if (matcher.matches()) {
      int major = Integer.parseInt(matcher.group(1));
      int minor = Integer.parseInt(matcher.group(2));
      int patch = matcher.group(4) == null ? 0 : Integer.parseInt(matcher.group(4));
      int hotfix = matcher.group(6) == null ? 0 : Integer.parseInt(matcher.group(6));
      return new Version(major, minor, patch, hotfix);
    }
    throw new IllegalArgumentException("Invalid version number: " + versionStr);
  }

  public static boolean isWithinRange(Version min, Version max, @NonNull Version def) {
    return ((min == null) && (max == null))
        || (((min == null) || (min.compareTo(def) <= 0))
            && ((max == null) || (max.compareTo(def) > 0)));
  }

  private final int major;
  private final int minor;
  private final int patch;
  private final int hotfix;

  private Version(int major, int minor, int patch, int hotfix) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.hotfix = hotfix;
  }

  @Override
  public int compareTo(@NonNull Version other) {
    if (major < other.major) {
      return -1;
    }
    if (major > other.major) {
      return 1;
    }
    if (minor < other.minor) {
      return -1;
    }
    if (minor > other.minor) {
      return 1;
    }
    if (patch < other.patch) {
      return -1;
    }
    if (patch > other.patch) {
      return 1;
    }
    return Integer.compare(hotfix, other.hotfix);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Version version = (Version) o;
    return major == version.major
        && minor == version.minor
        && patch == version.patch
        && hotfix == version.hotfix;
  }

  @Override
  public int hashCode() {
    int result = major;
    result = 31 * result + minor;
    result = 31 * result + patch;
    result = 31 * result + hotfix;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(major).append('.').append(minor).append('.').append(patch);
    if (hotfix > 0) {
      sb.append('.').append(hotfix);
    }
    return sb.toString();
  }
}
