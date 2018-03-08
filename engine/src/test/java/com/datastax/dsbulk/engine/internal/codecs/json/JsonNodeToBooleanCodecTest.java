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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonNodeToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();

  private final JsonNodeToBooleanCodec codec =
      new JsonNodeToBooleanCodec(inputs, newArrayList("NULL"));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.booleanNode(true))
        .to(true)
        .convertsFrom(JSON_NODE_FACTORY.booleanNode(false))
        .to(false)
        .convertsFrom(JSON_NODE_FACTORY.textNode("FOO"))
        .to(true)
        .convertsFrom(JSON_NODE_FACTORY.textNode("BAR"))
        .to(false)
        .convertsFrom(JSON_NODE_FACTORY.textNode("foo"))
        .to(true)
        .convertsFrom(JSON_NODE_FACTORY.textNode("bar"))
        .to(false)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode("NULL"))
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.nullNode())
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(true)
        .from(JSON_NODE_FACTORY.booleanNode(true))
        .convertsTo(false)
        .from(JSON_NODE_FACTORY.booleanNode(false))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid boolean"));
  }
}
