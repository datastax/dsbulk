/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.util.TemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class JsonNodeToInstantCodec extends JsonNodeToTemporalCodec<Instant> {

  private final ZoneId timeZone;
  private final ZonedDateTime epoch;

  public JsonNodeToInstantCodec(
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      ZonedDateTime epoch,
      List<String> nullStrings) {
    super(TypeCodecs.TIMESTAMP, temporalFormat, nullStrings);
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(JsonNode node) {
    TemporalAccessor temporal = parseTemporalAccessor(node);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toInstant(temporal, timeZone, epoch.toLocalDate());
  }
}
