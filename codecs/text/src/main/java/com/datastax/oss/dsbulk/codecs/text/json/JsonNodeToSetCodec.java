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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
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
