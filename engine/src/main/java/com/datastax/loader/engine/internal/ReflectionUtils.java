/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal;

/** */
public final class ReflectionUtils {

  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String fqdn) {
    try {
      Class<T> cl = (Class<T>) Class.forName(fqdn);
      return cl.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
}
