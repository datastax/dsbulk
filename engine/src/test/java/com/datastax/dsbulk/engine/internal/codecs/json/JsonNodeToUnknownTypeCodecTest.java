/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.engine.internal.codecs.string.StringToUnknownTypeCodecTest.Fruit;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToUnknownTypeCodecTest.FruitCodec;
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
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("banana"))
        .toInternal(banana)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToUnknownTypeCodec<Fruit> codec =
        new JsonNodeToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec).convertsFromInternal(banana).toExternal(JSON_NODE_FACTORY.textNode("banana"));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToUnknownTypeCodec<Fruit> codec =
        new JsonNodeToUnknownTypeCodec<>(targetCodec, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid fruit literal"));
  }
}
