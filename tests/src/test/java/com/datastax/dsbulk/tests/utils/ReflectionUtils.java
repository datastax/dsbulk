/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

/** */
public class ReflectionUtils {

  public static Set<Field> locateFieldsAnnotatedWith(
      Class<?> clazz, Class<? extends Annotation> annotationClass) {
    Set<Field> fields = new LinkedHashSet<Field>();
    while (!clazz.equals(Object.class)) {
      for (Field field : clazz.getDeclaredFields()) {
        if (!field.isSynthetic() && field.getAnnotation(annotationClass) != null) {
          fields.add(field);
        }
      }
      clazz = clazz.getSuperclass();
    }
    return fields;
  }

  public static <A extends Annotation> A locateClassAnnotation(
      Class<?> clazz, Class<A> annotationClass) {
    while (!clazz.equals(Object.class)) {
      A annotation = clazz.getAnnotation(annotationClass);
      if (annotation != null) return annotation;
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  public static Method locateMethod(String methodName, Class<?> clazz, int arity) {
    while (!clazz.equals(Object.class)) {
      for (Method method : clazz.getDeclaredMethods()) {
        // this won't work with overloaded methods
        if (method.getName().equals(methodName) && method.getParameterTypes().length == arity) {
          return method;
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  public static <T> T invokeMethod(
      Method method, Object receiver, Class<T> returnType, Object... parameters) {
    try {
      if (method == null) return null;
      method.setAccessible(true);

      // Void.isAssignableFrom always returns false it seems.
      if (returnType != Void.class) {
        assert returnType.isAssignableFrom(method.getReturnType());
      }
      return returnType.cast(method.invoke(receiver, parameters));
    } catch (Exception e) {
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
  }
}
