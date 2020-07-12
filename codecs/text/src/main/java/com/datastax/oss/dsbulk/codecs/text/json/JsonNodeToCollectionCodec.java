/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
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
