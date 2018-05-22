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

import com.datastax.driver.core.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToUnknownTypeCodec<T> extends JsonNodeConvertingCodec<T> {

  public JsonNodeToUnknownTypeCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, nullStrings);
  }

  @Override
  public T externalToInternal(JsonNode node) {
    if (isNull(node)) {
      return null;
    }
    return getInternalCodec().parse(node.asText());
  }

  @Override
  public JsonNode internalToExternal(T o) {
    if (o == null) {
      return null;
    }
    String s = getInternalCodec().format(o);
    // most codecs usually format null/empty values using the CQL keyword "NULL",
    // but some others may choose to return a null string.
    if (s == null || s.equalsIgnoreCase("NULL")) {
      return null;
    }
    return JSON_NODE_FACTORY.textNode(s);
  }
}
