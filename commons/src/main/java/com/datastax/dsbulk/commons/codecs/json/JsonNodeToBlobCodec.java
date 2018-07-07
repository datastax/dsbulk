/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.config.CodecSettings.JSON_NODE_FACTORY;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
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
    return value == null ? null : JSON_NODE_FACTORY.binaryNode(Bytes.getArray(value));
  }
}
