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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.ByteBuffer;
import java.util.List;

public class JsonNodeToBlobCodec extends ConvertingCodec<JsonNode, ByteBuffer> {

  private final List<String> nullWords;

  public JsonNodeToBlobCodec(List<String> nullWords) {
    super(blob(), JsonNode.class);
    this.nullWords = nullWords;
  }

  @Override
  public ByteBuffer convertFrom(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    String s = node.asText();
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public JsonNode convertTo(ByteBuffer value) {
    if (value == null) {
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.binaryNode(Bytes.getArray(value));
  }
}
