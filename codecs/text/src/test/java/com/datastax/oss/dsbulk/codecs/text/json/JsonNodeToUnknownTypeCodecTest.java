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

import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.dsbulk.codecs.text.string.StringToUnknownTypeCodecTest.Fruit;
import com.datastax.oss.dsbulk.codecs.text.string.StringToUnknownTypeCodecTest.FruitCodec;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToUnknownTypeCodecTest {

  private FruitCodec targetCodec = new FruitCodec();
  private List<String> nullStrings = newArrayList("NULL");
  private Fruit banana = new Fruit("banana");

  @Test
  void should_convert_from_valid_external() {
    JsonNodeToUnknownTypeCodec<Fruit> codec =
        new JsonNodeToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("banana"))
        .toInternal(banana)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToUnknownTypeCodec<Fruit> codec =
        new JsonNodeToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec)
        .convertsFromInternal(banana)
        .toExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("banana"));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToUnknownTypeCodec<Fruit> codec =
        new JsonNodeToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode("not a valid fruit literal"));
  }
}
