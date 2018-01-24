/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;

public class JsonNodeToBlobCodec extends ConvertingCodec<JsonNode, ByteBuffer> {

  public static final JsonNodeToBlobCodec INSTANCE = new JsonNodeToBlobCodec();

  private JsonNodeToBlobCodec() {
    super(blob(), JsonNode.class);
  }

  @Override
  public ByteBuffer convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public JsonNode convertTo(ByteBuffer value) {
    if (value == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    return JsonNodeFactory.instance.binaryNode(Bytes.getArray(value));
  }
}
