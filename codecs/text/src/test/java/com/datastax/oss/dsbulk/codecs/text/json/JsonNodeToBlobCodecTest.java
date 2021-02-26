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
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.format.binary.Base64BinaryFormat;
import com.datastax.oss.dsbulk.codecs.api.format.binary.HexBinaryFormat;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class JsonNodeToBlobCodecTest {

  private final byte[] data = {1, 2, 3, 4, 5, 6};
  private final byte[] empty = {};

  private final ByteBuffer dataBb = ByteBuffer.wrap(data);
  private final ByteBuffer emptyBb = ByteBuffer.wrap(empty);

  private final String data64 = Base64.getEncoder().encodeToString(data);
  private final String dataHex = Bytes.toHexString(data);

  private final String empty64 = Base64.getEncoder().encodeToString(empty);
  private final String emptyHex = Bytes.toHexString(empty);

  private final JsonNodeToBlobCodec codecBase64 =
      new JsonNodeToBlobCodec(Base64BinaryFormat.INSTANCE, Lists.newArrayList("NULL"));

  private final JsonNodeToBlobCodec codecHex =
      new JsonNodeToBlobCodec(HexBinaryFormat.INSTANCE, Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codecBase64)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(data))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(empty))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(data64))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(dataHex))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0x"))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        // DAT-573: consider empty string as empty byte array
        .toInternal(emptyBb)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    assertThat(codecHex)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(data))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.binaryNode(empty))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(data64))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(dataHex))
        .toInternal(dataBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0x"))
        .toInternal(emptyBb)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        // DAT-573: consider empty string as empty byte array
        .toInternal(emptyBb)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codecBase64)
        .convertsFromInternal(dataBb)
        .toExternal(JSON_NODE_FACTORY.textNode(data64))
        .convertsFromInternal(emptyBb)
        .toExternal(JSON_NODE_FACTORY.textNode(empty64))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codecHex)
        .convertsFromInternal(dataBb)
        .toExternal(JSON_NODE_FACTORY.textNode(dataHex))
        .convertsFromInternal(emptyBb)
        .toExternal(JSON_NODE_FACTORY.textNode(emptyHex))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codecBase64)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid binary"));
  }
}
