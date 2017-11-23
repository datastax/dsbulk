/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToCollectionCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;

public class StringToSetCodec<E> extends StringToCollectionCodec<E, Set<E>> {

  public StringToSetCodec(
      JsonNodeToCollectionCodec<E, Set<E>> jsonCodec, ObjectMapper objectMapper) {
    super(jsonCodec, objectMapper);
  }
}
