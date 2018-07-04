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

public final class ReflectionUtils {

  public static Object newInstance(String className)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
          NoSuchMethodException, InvocationTargetException {
    Class<?> cl = Class.forName(className);
    return newInstance(cl);
  }

  private static <T> T newInstance(Class<T> cl)
      throws IllegalAccessException, InstantiationException, NoSuchMethodException,
          InvocationTargetException {
    return cl.getConstructor().newInstance();
  }
}
