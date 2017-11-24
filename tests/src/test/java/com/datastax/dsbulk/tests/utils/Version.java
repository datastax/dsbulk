/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;

/**
 * This is a simplified version of the driver's VersionNumber class, to avoid references to the
 * driver jar.
 *
 * <p>It can only parse x.y.z.w version numbers.
 */
public class Version implements Comparable<Version> {

  public static final Version DEFAULT_OSS_VERSION;

  public static final Version DEFAULT_DSE_VERSION;

  private static final Pattern PATTERN = Pattern.compile("(\\d+)\\.(\\d+)(\\.(\\d+))?(\\.(\\d+))?");

  static {

    // These need other static fields to be initialized first

    DEFAULT_OSS_VERSION =
        parse(System.getProperty("com.datastax.dsbulk.tests.ccm.CASSANDRA_VERSION", "3.10"));

    DEFAULT_DSE_VERSION =
        parse(System.getProperty("com.datastax.dsbulk.tests.ccm.DSE_VERSION", "5.1.2"));
  }

  final int major;
  final int minor;
  final int patch;
  final int hotfix;

  public Version(int major, int minor, int patch, int hotfix) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.hotfix = hotfix;
  }

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

  @Override
  public int compareTo(@NotNull Version other) {
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
