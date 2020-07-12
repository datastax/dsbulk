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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToBlobCodec extends JsonNodeConvertingCodec<ByteBuffer> {

  public JsonNodeToBlobCodec(List<String> nullStrings) {
    super(TypeCodecs.BLOB, nullStrings);
  }

  @Override
  public ByteBuffer externalToInternal(JsonNode node) {
    // Do not test isNullOrEmpty(), it returns true for empty binary nodes
    if (isNull(node)) {
      return null;
    }
    if (node.isBinary()) {
      try {
        return ByteBuffer.wrap(node.binaryValue());
      } catch (IOException ignored) {
        // try as a string below
      }
    }
    String s = node.asText();
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public JsonNode internalToExternal(ByteBuffer value) {
    if (value == null) {
      return null;
    }
    byte[] array = Bytes.getArray(value);
    return JsonCodecUtils.JSON_NODE_FACTORY.binaryNode(array);
  }
}
