/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

public class JsonNodeToListCodec<E> extends JsonNodeToCollectionCodec<E, List<E>> {

  public JsonNodeToListCodec(
      TypeCodec<List<E>> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(collectionCodec, eltCodec, objectMapper, ArrayList::new, nullStrings, ImmutableList.of());
  }
}
