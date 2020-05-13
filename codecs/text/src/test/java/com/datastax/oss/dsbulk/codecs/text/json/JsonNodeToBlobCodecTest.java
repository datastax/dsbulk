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

  private final JsonNodeToBlobCodec codec = new JsonNodeToBlobCodec(Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
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
    assertThat(codec)
        .convertsFromInternal(dataBb)
        .toExternal(JSON_NODE_FACTORY.binaryNode(data))
        .convertsFromInternal(emptyBb)
        .toExternal(JSON_NODE_FACTORY.binaryNode(empty))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid binary"));
  }
}
