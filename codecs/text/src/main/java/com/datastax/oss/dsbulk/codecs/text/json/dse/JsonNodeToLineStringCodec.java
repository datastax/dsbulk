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
package com.datastax.oss.dsbulk.codecs.text.json.dse;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class JsonNodeToLineStringCodec extends JsonNodeConvertingCodec<LineString> {

  private final ObjectMapper objectMapper;

  public JsonNodeToLineStringCodec(ObjectMapper objectMapper, List<String> nullStrings) {
    super(DseTypeCodecs.LINE_STRING, nullStrings);
    this.objectMapper = objectMapper;
  }

  @Override
  public LineString externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    try {
      // We accept:
      // 1) String nodes containing WKT literals
      // 2) String nodes containing Geo JSON documents
      // 3) Json object nodes compliant with Geo JSON syntax
      // We need to serialize the node to support #3 above
      String s;
      if (node.isObject()) {
        s = objectMapper.writeValueAsString(node);
      } else {
        s = node.asText();
      }
      return CodecUtils.parseLineString(s);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot deserialize node " + node, e);
    }
  }

  @Override
  public JsonNode internalToExternal(LineString value) {
    if (value == null) {
      return null;
    }
    try {
      // Since geo types have a standardized Json format,
      // use that rather than WKT.
      return objectMapper.readTree(value.asGeoJson());
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot serialize value " + value, e);
    }
  }
}
