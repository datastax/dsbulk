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

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToDoubleCodecTest {

  private JsonNodeToDoubleCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToDoubleCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .build()
                .codecFor(DataTypes.DOUBLE, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0))
        .toInternal(0d)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(1234.56d))
        .toInternal(1234.56d)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(1.7976931348623157E308d))
        .toInternal(Double.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(4.9E-324d))
        .toInternal(Double.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal(0d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1234.56"))
        .toInternal(1234.56d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1,234.56"))
        .toInternal(1234.56d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1.7976931348623157E308"))
        .toInternal(Double.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("4.9E-324"))
        .toInternal(Double.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(0d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .toInternal(946684800000d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(1d)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(0d)
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
        .convertsFromInternal(0d)
        .toExternal(JSON_NODE_FACTORY.numberNode(0d))
        .convertsFromInternal(1234.56d)
        .toExternal(JSON_NODE_FACTORY.numberNode(1234.56d))
        .convertsFromInternal(0.001d)
        .toExternal(JSON_NODE_FACTORY.numberNode(0.001d))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid double"));
  }
}
