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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

public class ReflectionUtils {

  public static Set<Field> locateFieldsAnnotatedWith(
      Class<?> clazz, Class<? extends Annotation> annotationClass) {
    Set<Field> fields = new LinkedHashSet<>();
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
      if (annotation != null) {
        return annotation;
      }
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
      if (method == null) {
        return null;
      }
      method.setAccessible(true);

      // Void.isAssignableFrom always returns false it seems.
      assert returnType == Void.class || returnType.isAssignableFrom(method.getReturnType());
      return returnType.cast(method.invoke(receiver, parameters));
    } catch (Exception e) {
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  public static Field locateField(String fieldName, Class<?> clazz) {
    while (!clazz.equals(Object.class)) {
      for (Field f : clazz.getDeclaredFields()) {
        // this won't work with overloaded methods
        if (f.getName().equals(fieldName)) {
          return f;
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  public static Object getInternalState(Object target, String field) {
    Class<?> c = target.getClass();
    try {
      Field f = locateField(field, c);
      assert f != null : String.format("Cannot find field %s in target %s", field, target);
      f.setAccessible(true);
      return f.get(target);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to get internal state on a private field. Please report to mockito mailing list.",
          e);
    }
  }

  public static Object getInternalState(Class<?> c, String field) {
    try {
      Field f = locateField(field, c);
      assert f != null : String.format("Cannot find field %s in target %s", field, c);
      f.setAccessible(true);
      return f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to get internal state on a private field. Please report to mockito mailing list.",
          e);
    }
  }

  public static void setInternalState(Object target, String field, Object value) {
    Class<?> c = target.getClass();
    try {
      Field f = locateField(field, c);
      assert f != null;
      f.setAccessible(true);
      f.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to set internal state on a private field. Please report to mockito mailing list.",
          e);
    }
  }
}
