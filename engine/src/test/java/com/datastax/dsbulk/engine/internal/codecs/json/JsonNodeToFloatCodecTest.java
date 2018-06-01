/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.DataType.cfloat;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToFloatCodecTest {

  private JsonNodeToFloatCodec codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToFloatCodec)
            newCodecRegistry("nullStrings = [NULL]")
                .codecFor(cfloat(), TypeToken.of(JsonNode.class));
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0f))
        .toInternal(0f)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(1234.56f))
        .toInternal(1234.56f)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(3.4028235E38f))
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(1.4E-45f))
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal(
            JSON_NODE_FACTORY.numberNode(340_282_346_638_528_860_000_000_000_000_000_000_000f))
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal(
            JSON_NODE_FACTORY.numberNode(0.0000000000000000000000000000000000000000000014f))
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal(0f)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1234.56"))
        .toInternal(1234.56f)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1,234.56"))
        .toInternal(1234.56f)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("3.4028235E38"))
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1.4E-45"))
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("340,282,350,000,000,000,000,000,000,000,000,000,000"))
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("0.0000000000000000000000000000000000000000000014"))
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(0f)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(1f)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(0f)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0f)
        .toExternal(JSON_NODE_FACTORY.numberNode(0f))
        .convertsFromInternal(1234.56f)
        .toExternal(JSON_NODE_FACTORY.numberNode(1234.56f))
        .convertsFromInternal(Float.MAX_VALUE)
        .toExternal(
            JSON_NODE_FACTORY.numberNode(340_282_350_000_000_000_000_000_000_000_000_000_000f))
        .convertsFromInternal(0.001f)
        .toExternal(JSON_NODE_FACTORY.numberNode(0.001f))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid float"));
  }
}
