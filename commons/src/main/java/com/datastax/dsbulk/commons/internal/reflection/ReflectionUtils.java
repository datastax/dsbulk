/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.reflection;

import java.util.Arrays;
import java.util.List;

/** */
public final class ReflectionUtils {

  private static final List<String> PACKAGE_PREFIXES =
      Arrays.asList("", "com.datastax.driver.core.policies.", "com.datastax.driver.core.");

  public static <T> T newInstance(String className)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Class<T> cl = resolveClass(className);
    return newInstance(cl);
  }

  @SuppressWarnings("WeakerAccess")
  public static <T> T newInstance(Class<T> cl)
      throws IllegalAccessException, InstantiationException {
    return cl.newInstance();
  }

  public static <T> Class<T> resolveClass(String className) throws ClassNotFoundException {
    for (String packagePrefix : PACKAGE_PREFIXES) {
      String fqcn = packagePrefix + className;
      try {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) Class.forName(fqcn);
        return clazz;
      } catch (ClassNotFoundException e) {
        // swallow
      }
    }
    throw new ClassNotFoundException("Class " + className + " not found");
  }
}
