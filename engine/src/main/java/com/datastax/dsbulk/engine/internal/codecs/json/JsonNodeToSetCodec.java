/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashSet;
import java.util.Set;

public class JsonNodeToSetCodec<E> extends JsonNodeToCollectionCodec<E, Set<E>> {

  public JsonNodeToSetCodec(
      TypeCodec<Set<E>> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper) {
    super(collectionCodec, eltCodec, objectMapper, LinkedHashSet::new);
  }
}
