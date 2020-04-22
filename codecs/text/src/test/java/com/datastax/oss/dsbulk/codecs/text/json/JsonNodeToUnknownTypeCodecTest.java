/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
