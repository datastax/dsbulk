/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;

/** */
@SuppressWarnings("WeakerAccess")
public class VersionUtils {

  public static final String DEFAULT_OSS_VERSION = "3.12";
  public static final String DEFAULT_DSE_VERSION = "5.1.2";

  /**
   * A mapping of full DSE versions to their C* counterpart. This is not meant to be comprehensive.
   * If C* version cannot be derived, the method makes a 'best guess'.
   */
  private static final Map<String, String> DSE_TO_OSS_VERSIONS;

  static {
    Map<String, String> map = new HashMap<>();
    map.put("5.1", "3.10");
    map.put("5.0", "3.0");
    map.put("4.8.3", "2.1.11");
    map.put("4.8.2", "2.1.11");
    map.put("4.8.1", "2.1.11");
    map.put("4.8", "2.1.9");
    map.put("4.7.6", "2.1.11");
    map.put("4.7.5", "2.1.11");
    map.put("4.7.4", "2.1.11");
    map.put("4.7.3", "2.1.8");
    map.put("4.7.2", "2.1.8");
    map.put("4.7.1", "2.1.5");
    map.put("4.6.11", "2.0.16");
    map.put("4.6.10", "2.0.16");
    map.put("4.6.9", "2.0.16");
    map.put("4.6.8", "2.0.16");
    map.put("4.6.7", "2.0.14");
    map.put("4.6.6", "2.0.14");
    map.put("4.6.5", "2.0.14");
    map.put("4.6.4", "2.0.14");
    map.put("4.6.3", "2.0.12");
    map.put("4.6.2", "2.0.12");
    map.put("4.6.1", "2.0.12");
    map.put("4.6", "2.0.11");
    map.put("4.5.9", "2.0.16");
    map.put("4.5.8", "2.0.14");
    map.put("4.5.7", "2.0.12");
    map.put("4.5.6", "2.0.12");
    map.put("4.5.5", "2.0.12");
    map.put("4.5.4", "2.0.11");
    map.put("4.5.3", "2.0.11");
    map.put("4.5.2", "2.0.10");
    map.put("4.5.1", "2.0.8");
    map.put("4.5", "2.0.8");
    map.put("4.0", "2.0");
    map.put("3.2", "1.2");
    map.put("3.1", "1.2");
    DSE_TO_OSS_VERSIONS = Collections.unmodifiableMap(map);
  }

  private static final Pattern VERSION_RANGE_PATTERN =
      Pattern.compile("\\[([0-9.]*),([0-9.]*)]");

  /**
   * @return The cassandra version matching the given DSE version. If the DSE version can't be
   *     derived the following logic is used:
   *     <ol>
   *       <li>If <= 3.X, use C* 1.2
   *       <li>If 4.X, use 2.1 for >= 4.7, 2.0 otherwise.
   *       <li>Otherwise 3.0
   *     </ol>
   */
  public static String getOSSVersionForDSEVersion(String dseVersion) {
    String cassandraVersion = DSE_TO_OSS_VERSIONS.get(dseVersion);
    if (cassandraVersion != null) {
      return cassandraVersion;
    }
    Version dseVersionNumber = Version.parse(dseVersion);
    if (dseVersionNumber.major <= 3) {
      return "1.2";
    } else if (dseVersionNumber.major == 4) {
      if (dseVersionNumber.compareTo(Version.parse("4.7")) >= 0) {
        return "2.1";
      } else {
        return "2.0";
      }
    } else {
      // Fallback on 3.0 by default.
      return "3.0";
    }
  }

  public static String getBestVersion(String singleVersionOrVersionRange, boolean dse) {
    if (dse) return getBestDSEVersion(singleVersionOrVersionRange);
    else return getBestOSSVersion(singleVersionOrVersionRange);
  }

  public static String getBestOSSVersion(String singleVersionOrVersionRange) {
    return getBestVersion(singleVersionOrVersionRange, DEFAULT_OSS_VERSION);
  }

  public static String getBestDSEVersion(String singleVersionOrVersionRange) {
    return getBestVersion(singleVersionOrVersionRange, DEFAULT_DSE_VERSION);
  }

  private static String getBestVersion(String singleVersionOrVersionRange, String defaultVersion) {
    if (singleVersionOrVersionRange == null) return defaultVersion;
    Matcher matcher = VERSION_RANGE_PATTERN.matcher(singleVersionOrVersionRange);
    String startVersion;
    String endVersion;
    if (matcher.matches()) {
      startVersion = matcher.group(1).isEmpty() ? null : matcher.group(1);
      endVersion = matcher.group(2).isEmpty() ? null : matcher.group(2);
      if (startVersion == null && endVersion == null) {
        throw new IllegalArgumentException("Invalid version range: " + singleVersionOrVersionRange);
      }
      // check syntax
      if (startVersion != null) Version.parse(startVersion);
      if (endVersion != null) Version.parse(endVersion);
    } else {
      // should be a single version; check syntax
      Version.parse(singleVersionOrVersionRange);
      startVersion = endVersion = singleVersionOrVersionRange;
    }
    // if default within allowed range return default
    if (isWithinRange(defaultVersion, startVersion, endVersion)) return defaultVersion;
    // else return maximum version within range, if it exists
    if (endVersion != null) return endVersion;
    // else return minimum version within range
    return startVersion;
  }

  public static boolean isWithinRange(
      String versionStr, String lowerBoundInclusiveStr, String upperBoundInclusiveStr) {
    Version version = Version.parse(versionStr);
    Version lowerBoundInclusive = Version.parse(lowerBoundInclusiveStr);
    Version upperBoundInclusive = Version.parse(upperBoundInclusiveStr);
    return (lowerBoundInclusive == null || version.compareTo(lowerBoundInclusive) >= 0)
        && (upperBoundInclusive == null || version.compareTo(upperBoundInclusive) <= 0);
  }

  public static int compare(String v1, String v2) {
    return Version.parse(v1).compareTo(Version.parse(v2));
  }

  public static String computeVersion(CCMConfig config, boolean dse) {
    if (config != null && !config.version().equals("")) {
      return VersionUtils.getBestVersion(config.version(), dse);
    }
    String systemDSE = System.getProperty("com.datastax.dsbulk.tests.ccm.DSE_VERSION");
    if (systemDSE != null) {
      return VersionUtils.getBestVersion(systemDSE, dse);
    }

    return VersionUtils.getBestVersion(VersionUtils.DEFAULT_DSE_VERSION, dse);
  }

  /**
   * This is a simplified version of the driver's VersionNumber class, to avoid references to the
   * driver jar.
   *
   * <p>It can only parse x.y.z.w version numbers.
   */
  private static class Version implements Comparable<Version> {

    private static final Pattern PATTERN =
        Pattern.compile("(\\d+)\\.(\\d+)(\\.(\\d+))?(\\.(\\d+))?");

    final int major;
    final int minor;
    final int patch;
    final int hotfix;

    Version(int major, int minor, int patch, int hotfix) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.hotfix = hotfix;
    }

    static Version parse(String versionStr) {
      if (versionStr == null) return null;
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
      if (major < other.major) return -1;
      if (major > other.major) return 1;
      if (minor < other.minor) return -1;
      if (minor > other.minor) return 1;
      if (patch < other.patch) return -1;
      if (patch > other.patch) return 1;
      return Integer.compare(hotfix, other.hotfix);
    }
  }
}
