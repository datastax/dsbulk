/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import java.util.LinkedHashMap;

public class DefaultMapping extends LinkedHashMap<Object, String> implements Mapping {

  @Override
  public String map(Object field) {
    return get(field);
  }
}
