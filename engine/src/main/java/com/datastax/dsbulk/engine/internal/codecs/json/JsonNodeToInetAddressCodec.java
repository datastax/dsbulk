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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetAddress;
import java.util.List;

public class JsonNodeToInetAddressCodec extends ConvertingCodec<JsonNode, InetAddress> {

  private final List<String> nullWords;

  public JsonNodeToInetAddressCodec(List<String> nullWords) {
    super(inet(), JsonNode.class);
    this.nullWords = nullWords;
  }

  @Override
  public InetAddress externalToInternal(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    String s = node.asText();
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new InvalidTypeException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public JsonNode internalToExternal(InetAddress value) {
    if (value == null) {
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.textNode(value.getHostAddress());
  }
}
