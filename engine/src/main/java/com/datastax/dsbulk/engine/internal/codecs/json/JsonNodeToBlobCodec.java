/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToBlobCodec extends JsonNodeConvertingCodec<ByteBuffer> {

  public JsonNodeToBlobCodec(List<String> nullStrings) {
    super(blob(), nullStrings);
  }

  @Override
  public ByteBuffer externalToInternal(JsonNode node) {
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
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.binaryNode(Bytes.getArray(value));
  }
}
