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
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToBigDecimalCodecTest {

  JsonNodeToBigDecimalCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToBigDecimalCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(DataTypes.DECIMAL, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0))
        .toInternal(ZERO)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0d))
        .toInternal(new BigDecimal("0.0"))
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(ONE))
        .toInternal(ONE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(-1234.56))
        .toInternal(new BigDecimal("-1234.56"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-1,234.56"))
        .toInternal(new BigDecimal("-1234.56"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(new BigDecimal("0"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .toInternal(new BigDecimal("946684800000"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(ONE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(ZERO)
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
        .convertsFromInternal(ZERO)
        .toExternal(JSON_NODE_FACTORY.numberNode(ZERO))
        .convertsFromInternal(new BigDecimal("1234.56"))
        .toExternal(JSON_NODE_FACTORY.numberNode(new BigDecimal("1234.56")))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid decimal"));
  }
}
