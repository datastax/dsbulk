/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public abstract class JsonNodeToCollectionCodec<E, C extends Collection<E>>
    extends ConvertingCodec<JsonNode, C> {

  private final ConvertingCodec<JsonNode, E> eltCodec;
  private final Supplier<C> collectionSupplier;
  private final ObjectMapper objectMapper;
  private final List<String> nullWords;

  JsonNodeToCollectionCodec(
      TypeCodec<C> collectionCodec,
      ConvertingCodec<JsonNode, E> eltCodec,
      ObjectMapper objectMapper,
      Supplier<C> collectionSupplier,
      List<String> nullWords) {
    super(collectionCodec, JsonNode.class);
    this.eltCodec = eltCodec;
    this.objectMapper = objectMapper;
    this.collectionSupplier = collectionSupplier;
    this.nullWords = nullWords;
  }

  @Override
  public C convertFrom(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    if (!node.isArray()) {
      throw new InvalidTypeException("Expecting ARRAY node, got " + node.getNodeType());
    }
    if (node.size() == 0) {
      return null;
    }
    Iterator<JsonNode> elements = node.elements();
    C collection = collectionSupplier.get();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      collection.add(eltCodec.convertFrom(element));
    }
    return collection;
  }

  @Override
  public JsonNode convertTo(C value) {
    if (value == null) {
      return objectMapper.getNodeFactory().nullNode();
    }
    ArrayNode root = objectMapper.createArrayNode();
    for (E element : value) {
      root.add(eltCodec.convertTo(element));
    }
    return root;
  }
}
