/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_TYPE;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToFloatCodecTest {

  private JsonNodeToFloatCodec codec;

  @BeforeEach
  void setUp() {
    ConversionContext context = new TextConversionContext().setNullStrings("NULL");
    ConvertingCodecFactory codecFactory = new ConvertingCodecFactory(context);
    codec =
        (JsonNodeToFloatCodec)
            codecFactory.<JsonNode, Float>createConvertingCodec(
                DataTypes.FLOAT, JSON_NODE_TYPE, true);
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
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
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
