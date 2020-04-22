/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConversionContext {

  private final ConcurrentMap<String, Object> attributes = new ConcurrentHashMap<>();

  public void addAttribute(@NonNull String key, Object value) {
    attributes.put(key, value);
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public <T> T getAttribute(@NonNull String key) {
    return (T) attributes.get(key);
  }
}
