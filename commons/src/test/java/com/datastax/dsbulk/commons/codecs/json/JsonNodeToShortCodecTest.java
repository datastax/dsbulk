/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.config.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToShortCodecTest {

  private JsonNodeToShortCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToShortCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(DataTypes.SMALLINT, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) 0))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) 32767))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) -32768))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("32767"))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-32768"))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("32,767"))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-32,768"))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal((short) 1)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal((short) 0)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) 0))
        .convertsFromInternal(Short.MAX_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) 32_767))
        .convertsFromInternal(Short.MIN_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) -32_768))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid short"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("32768"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("-32769"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
