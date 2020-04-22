/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class JsonNodeToUUIDCodec extends JsonNodeConvertingCodec<UUID> {

  private final ConvertingCodec<String, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public JsonNodeToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      ConvertingCodec<String, Instant> instantCodec,
      TimeUUIDGenerator generator,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.instantCodec = instantCodec;
    this.generator = generator;
  }

  @Override
  public UUID externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    return CodecUtils.parseUUID(node.asText(), instantCodec, generator);
  }

  @Override
  public JsonNode internalToExternal(UUID value) {
    return value == null ? null : JsonCodecUtils.JSON_NODE_FACTORY.textNode(value.toString());
  }
}
