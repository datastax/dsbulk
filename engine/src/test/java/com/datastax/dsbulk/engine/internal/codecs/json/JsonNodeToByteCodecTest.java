/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.DataType.tinyint;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToByteCodecTest {

  private JsonNodeToByteCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToByteCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(tinyint(), TypeToken.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((byte) 0))
        .toInternal((byte) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((byte) 127))
        .toInternal((byte) 127)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((byte) -128))
        .toInternal((byte) -128)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal((byte) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("127"))
        .toInternal((byte) 127)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-128"))
        .toInternal((byte) -128)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal((byte) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal((byte) 1)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal((byte) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal((byte) 0)
        .toExternal(JSON_NODE_FACTORY.numberNode((byte) 0))
        .convertsFromInternal((byte) 127)
        .toExternal(JSON_NODE_FACTORY.numberNode((byte) 127))
        .convertsFromInternal((byte) -128)
        .toExternal(JSON_NODE_FACTORY.numberNode((byte) -128))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid byte"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.numberNode(1.2))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.numberNode(128))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.numberNode(-129))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
