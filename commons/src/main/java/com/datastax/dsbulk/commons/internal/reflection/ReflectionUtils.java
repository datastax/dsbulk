/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/** */
public final class ReflectionUtils {

  private static final List<String> PACKAGE_PREFIXES =
      Arrays.asList("", "com.datastax.driver.core.policies.", "com.datastax.driver.core.");

  public static <T> T newInstance(String className)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
          NoSuchMethodException, InvocationTargetException {
    Class<T> cl = resolveClass(className);
    return newInstance(cl);
  }

  @SuppressWarnings("WeakerAccess")
  public static <T> T newInstance(Class<T> cl)
      throws IllegalAccessException, InstantiationException, NoSuchMethodException,
          InvocationTargetException {
    return cl.getConstructor().newInstance();
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
