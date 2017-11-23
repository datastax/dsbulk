/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToCollectionCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class StringToListCodec<E> extends StringToCollectionCodec<E, List<E>> {

  public StringToListCodec(
      JsonNodeToCollectionCodec<E, List<E>> jsonCodec, ObjectMapper objectMapper) {
    super(jsonCodec, objectMapper);
  }
}
