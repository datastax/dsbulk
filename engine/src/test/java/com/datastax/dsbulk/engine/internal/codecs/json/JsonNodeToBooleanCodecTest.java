/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonNodeToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();
  private final JsonNodeToBooleanCodec codec = new JsonNodeToBooleanCodec(inputs);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.booleanNode(true))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.booleanNode(false))
        .to(false)
        .convertsFrom(JsonNodeFactory.instance.textNode("FOO"))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.textNode("BAR"))
        .to(false)
        .convertsFrom(JsonNodeFactory.instance.textNode("foo"))
        .to(true)
        .convertsFrom(JsonNodeFactory.instance.textNode("bar"))
        .to(false)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.nullNode())
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(true)
        .from(JsonNodeFactory.instance.booleanNode(true))
        .convertsTo(false)
        .from(JsonNodeFactory.instance.booleanNode(false))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid boolean"));
  }
}
