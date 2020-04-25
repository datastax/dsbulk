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
import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetAddress;
import java.util.List;

public class JsonNodeToInetAddressCodec extends JsonNodeConvertingCodec<InetAddress> {

  public JsonNodeToInetAddressCodec(List<String> nullStrings) {
    super(TypeCodecs.INET, nullStrings);
  }

  @Override
  public InetAddress externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    String s = node.asText();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Cannot create inet address from empty string");
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public JsonNode internalToExternal(InetAddress value) {
    return value == null ? null : JsonCodecUtils.JSON_NODE_FACTORY.textNode(value.getHostAddress());
  }
}
