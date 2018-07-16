/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
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
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.booleanNode(true))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.booleanNode(false))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FOO"))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("BAR"))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("foo"))
        .toInternal(true)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("bar"))
        .toInternal(false)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(true)
        .toExternal(JSON_NODE_FACTORY.booleanNode(true))
        .convertsFromInternal(false)
        .toExternal(JSON_NODE_FACTORY.booleanNode(false))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid boolean"));
  }
}
