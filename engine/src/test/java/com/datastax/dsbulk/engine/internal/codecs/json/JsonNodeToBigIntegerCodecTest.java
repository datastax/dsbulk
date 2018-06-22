/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToBigIntegerCodecTest {

  private JsonNodeToBigIntegerCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToBigIntegerCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(DataTypes.VARINT, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0))
        .toInternal(BigInteger.ZERO)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0d))
        .toInternal(new BigInteger("0"))
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(BigInteger.ONE))
        .toInternal(BigInteger.ONE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(-1234))
        .toInternal(new BigInteger("-1234"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-1,234"))
        .toInternal(new BigInteger("-1234"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(new BigInteger("0"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .toInternal(new BigInteger("946684800000"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(BigInteger.ONE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(BigInteger.ZERO)
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
        .convertsFromInternal(BigInteger.ZERO)
        .toExternal(JSON_NODE_FACTORY.numberNode(BigInteger.ZERO))
        .convertsFromInternal(new BigInteger("-1234"))
        .toExternal(JSON_NODE_FACTORY.numberNode(new BigInteger("-1234")))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid biginteger"));
  }
}
