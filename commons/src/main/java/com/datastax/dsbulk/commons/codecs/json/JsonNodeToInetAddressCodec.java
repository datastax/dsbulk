/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetAddress;
import java.util.List;

public class JsonNodeToInetAddressCodec extends JsonNodeConvertingCodec<InetAddress> {

  public JsonNodeToInetAddressCodec(List<String> nullStrings) {
    super(TypeCodecs.INET, nullStrings);
  }

  @Override
  public InetAddress externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    String s = node.asText();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Cannot create inet address from empty string");
    }
    try {
      return InetAddress.getByName(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse inet address: " + s);
    }
  }

  @Override
  public JsonNode internalToExternal(InetAddress value) {
    return value == null ? null : JSON_NODE_FACTORY.textNode(value.getHostAddress());
  }
}
