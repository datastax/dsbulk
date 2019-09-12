/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.reflection;

import java.util.Arrays;
import java.util.List;

public final class ReflectionUtils {

  private static final List<String> PACKAGE_PREFIXES =
      Arrays.asList(
          "",
          "com.datastax.oss.driver.internal.core.addresstranslation.",
          "com.datastax.oss.driver.internal.core.auth.",
          "com.datastax.oss.driver.internal.core.loadbalancing.",
          "com.datastax.oss.driver.internal.core.retry.",
          "com.datastax.oss.driver.internal.core.specex.",
          "com.datastax.oss.driver.internal.core.ssl.",
          "com.datastax.oss.driver.internal.core.time.");

  public static Class<?> resolveClass(String className) throws ClassNotFoundException {
    for (String packagePrefix : PACKAGE_PREFIXES) {
      String fqcn = packagePrefix + className;
      try {
        return Class.forName(fqcn);
      } catch (ClassNotFoundException e) {
        // swallow
      }
    }
    throw new ClassNotFoundException("Class " + className + " not found");
  }
}
