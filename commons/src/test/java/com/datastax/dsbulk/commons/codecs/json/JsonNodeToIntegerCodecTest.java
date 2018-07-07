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

class JsonNodeToIntegerCodecTest {

  private JsonNodeToIntegerCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToIntegerCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(DataTypes.INT, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0))
        .toInternal(0)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(2_147_483_647))
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(-2_147_483_648))
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal(0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2147483647"))
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-2147483648"))
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2,147,483,647"))
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-2,147,483,648"))
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(1)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(0)
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
        .convertsFromInternal(0)
        .toExternal(JSON_NODE_FACTORY.numberNode(0))
        .convertsFromInternal(Integer.MAX_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(2_147_483_647))
        .convertsFromInternal(Integer.MIN_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(-2_147_483_648))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid integer"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("2147483648"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("-2147483649"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
