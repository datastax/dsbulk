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
import java.util.ArrayList;
import java.util.List;

public class JsonNodeToListCodec<E> extends JsonNodeToCollectionCodec<E, List<E>> {

  public JsonNodeToListCodec(
      TypeCodec<List<E>> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper) {
    super(collectionCodec, eltCodec, objectMapper, ArrayList::new);
  }
}
