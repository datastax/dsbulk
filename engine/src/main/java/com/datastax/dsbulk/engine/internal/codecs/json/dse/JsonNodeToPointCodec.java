/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json.dse;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.dse.geometry.Point;
import com.datastax.driver.dse.geometry.codecs.PointCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class JsonNodeToPointCodec extends JsonNodeConvertingCodec<Point> {

  private final ObjectMapper objectMapper;

  public JsonNodeToPointCodec(ObjectMapper objectMapper, List<String> nullStrings) {
    super(PointCodec.INSTANCE, nullStrings);
    this.objectMapper = objectMapper;
  }

  @Override
  public Point externalToInternal(JsonNode node) {
    if (isNull(node)) {
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
      return CodecUtils.parsePoint(s);
    } catch (JsonProcessingException e) {
      throw new InvalidTypeException("Cannot deserialize node " + node, e);
    }
  }

  @Override
  public JsonNode internalToExternal(Point value) {
    if (value == null) {
      return null;
    }
    try {
      // Since geo types have a standardized Json format,
      // use that rather than WKT.
      return objectMapper.readTree(value.asGeoJson());
    } catch (IOException e) {
      throw new InvalidTypeException("Cannot serialize value " + value, e);
    }
  }
}
