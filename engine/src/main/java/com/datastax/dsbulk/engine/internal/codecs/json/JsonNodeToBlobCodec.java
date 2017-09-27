/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;
import java.util.Base64;

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
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return Bytes.fromHexString(s);
    } catch (IllegalArgumentException e) {
      try {
        return ByteBuffer.wrap(Base64.getDecoder().decode(s));
      } catch (IllegalArgumentException e1) {
        e1.addSuppressed(e);
        throw new InvalidTypeException("Invalid binary string: " + s, e1);
      }
    }
  }

  @Override
  public JsonNode convertTo(ByteBuffer value) {
    if (value == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    return JsonNodeFactory.instance.binaryNode(Bytes.getArray(value));
  }
}
