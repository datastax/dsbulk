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

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToStringCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToStringCodec codec =
        new JsonNodeToStringCodec(TypeCodec.varchar(), objectMapper, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("foo"))
        .toInternal("foo")
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("\"foo\""))
        .toInternal("\"foo\"")
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("'foo'"))
        .toInternal("'foo'")
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(42))
        .toInternal("42")
        .convertsFromExternal(objectMapper.readTree("{\"foo\":42}"))
        .toInternal("{\"foo\":42}")
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal("[1,2,3]")
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal("")
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws IOException {
    JsonNodeToStringCodec codec =
        new JsonNodeToStringCodec(TypeCodec.varchar(), objectMapper, nullStrings);
    assertThat(codec)
        .convertsFromInternal("foo")
        .toExternal(JSON_NODE_FACTORY.textNode("foo"))
        .convertsFromInternal("\"foo\"")
        .toExternal(JSON_NODE_FACTORY.textNode("\"foo\""))
        .convertsFromInternal("'foo'")
        .toExternal(JSON_NODE_FACTORY.textNode("'foo'"))
        .convertsFromInternal("42")
        .toExternal(JSON_NODE_FACTORY.numberNode(42))
        .convertsFromInternal("{\"foo\":42}")
        .toExternal(objectMapper.readTree("{\"foo\":42}"))
        .convertsFromInternal("[1,2,3]")
        .toExternal(objectMapper.readTree("[1,2,3]"))
        .convertsFromInternal(null)
        .toExternal(null)
        .convertsFromInternal("")
        .toExternal(JSON_NODE_FACTORY.textNode(""));
  }
}
