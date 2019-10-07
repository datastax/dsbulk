/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class JsonNodeToLocalTimeCodec extends JsonNodeToTemporalCodec<LocalTime> {

  private final ZoneId timeZone;

  public JsonNodeToLocalTimeCodec(
      TemporalFormat parser, ZoneId timeZone, List<String> nullStrings) {
    super(TypeCodecs.TIME, parser, nullStrings);
    this.timeZone = timeZone;
  }

  @Override
  public LocalTime externalToInternal(JsonNode s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toLocalTime(temporal, timeZone);
  }
}
