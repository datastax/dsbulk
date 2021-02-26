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

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.format.geo.GeoFormat;
import com.datastax.oss.dsbulk.codecs.api.format.geo.JsonGeoFormat;
import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.List;

public abstract class JsonNodeToGeometryCodec<T extends Geometry>
    extends JsonNodeConvertingCodec<T> {

  protected final ObjectMapper objectMapper;
  protected final GeoFormat geoFormat;

  protected JsonNodeToGeometryCodec(
      TypeCodec<T> targetCodec,
      ObjectMapper objectMapper,
      GeoFormat geoFormat,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.objectMapper = objectMapper;
    this.geoFormat = geoFormat;
  }

  @Override
  public T externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    try {
      // We accept:
      // 1) String nodes containing WKT literals
      // 2) String nodes containing Geo JSON documents
      // 3) String nodes containing WKB encoded strings
      // 4) Json object nodes compliant with Geo JSON syntax
      // 5) Binary nodes containing WKB strings
      String s;
      if (node.isObject()) {
        s = objectMapper.writeValueAsString(node);
        return parseGeometry(s);
      } else if (node.isBinary()) {
        byte[] bytes = node.binaryValue();
        return parseGeometry(bytes);
      } else {
        s = node.asText();
        return parseGeometry(s);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot deserialize node " + node, e);
    }
  }

  protected abstract T parseGeometry(@NonNull String s);

  protected abstract T parseGeometry(@NonNull byte[] b);

  @Override
  public JsonNode internalToExternal(T value) {
    if (value == null) {
      return null;
    }
    try {
      String s = geoFormat.format(value);
      if (geoFormat == JsonGeoFormat.INSTANCE) {
        // Return a JSON object for GeoJson format instead of a text node
        return objectMapper.readTree(s);
      } else {
        // Return a text node for Well-known text and Well-known binary formats.
        return objectMapper.getNodeFactory().textNode(s);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot serialize value " + value, e);
    }
  }
}
