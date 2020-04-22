/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class JsonNodeToSetCodec<E> extends JsonNodeToCollectionCodec<E, Set<E>> {

  public JsonNodeToSetCodec(
      TypeCodec<Set<E>> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(
        collectionCodec,
        eltCodec,
        objectMapper,
        LinkedHashSet::new,
        nullStrings,
        ImmutableSet.of());
  }
}
