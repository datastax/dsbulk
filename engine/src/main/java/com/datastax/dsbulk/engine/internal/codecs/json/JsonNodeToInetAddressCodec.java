/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.InetAddress;

public class JsonNodeToInetAddressCodec extends ConvertingCodec<JsonNode, InetAddress> {

  public static final JsonNodeToInetAddressCodec INSTANCE = new JsonNodeToInetAddressCodec();

  private JsonNodeToInetAddressCodec() {
    super(inet(), JsonNode.class);
  }

  @Override
  public InetAddress convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new InvalidTypeException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public JsonNode convertTo(InetAddress value) {
    if (value == null) {
      return null;
    }
    return JsonNodeFactory.instance.textNode(value.getHostAddress());
  }
}
