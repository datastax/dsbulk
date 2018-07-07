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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToDurationCodec extends JsonNodeConvertingCodec<CqlDuration> {

  public JsonNodeToDurationCodec(List<String> nullStrings) {
    super(TypeCodecs.DURATION, nullStrings);
  }

  @Override
  public CqlDuration externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    String s = node.asText();
    return CqlDuration.from(s);
  }

  @Override
  public JsonNode internalToExternal(CqlDuration value) {
    return value == null ? null : JSON_NODE_FACTORY.textNode(value.toString());
  }
}
