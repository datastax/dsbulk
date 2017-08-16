/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal;

import java.util.Arrays;
import java.util.List;

/** */
public final class ReflectionUtils {

  private static final List<String> PACKAGE_PREFIXES =
      Arrays.asList("", "com.datastax.driver.core.policies.", "com.datastax.driver.core.");

  public static <T> T newInstance(String className) {
    Class<T> cl = resolveClass(className);
    return newInstance(cl);
  }

  public static <T> T newInstance(Class<T> cl) {
    try {
      return cl.newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> Class<T> resolveClass(String className) {
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
    throw new RuntimeException(new ClassNotFoundException("Class " + className + " not found"));
  }
}
