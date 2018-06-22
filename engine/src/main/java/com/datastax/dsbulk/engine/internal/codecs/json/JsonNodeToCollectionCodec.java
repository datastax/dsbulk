/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public abstract class JsonNodeToCollectionCodec<E, C extends Collection<E>>
    extends JsonNodeConvertingCodec<C> {

  private final ConvertingCodec<JsonNode, E> eltCodec;
  private final Supplier<C> collectionSupplier;
  private final ObjectMapper objectMapper;
  private final C emptyCollection;

  JsonNodeToCollectionCodec(
      TypeCodec<C> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper,
      Supplier<C> collectionSupplier,
      List<String> nullStrings,
      C emptyCollection) {
    super(collectionCodec, nullStrings);
    this.eltCodec = eltCodec;
    this.objectMapper = objectMapper;
    this.collectionSupplier = collectionSupplier;
    this.emptyCollection = emptyCollection;
  }

  @Override
  public C externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!node.isArray()) {
      throw new IllegalArgumentException("Expecting ARRAY node, got " + node.getNodeType());
    }
    if (node.size() == 0) {
      return emptyCollection;
    }
    Iterator<JsonNode> elements = node.elements();
    C collection = collectionSupplier.get();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      collection.add(eltCodec.externalToInternal(element));
    }
    return collection;
  }

  @Override
  public JsonNode internalToExternal(C value) {
    if (value == null) {
      return null;
    }
    ArrayNode root = objectMapper.createArrayNode();
    for (E element : value) {
      root.add(eltCodec.internalToExternal(element));
    }
    return root;
  }
}
