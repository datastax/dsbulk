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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class JsonNodeToLocalDateCodec extends JsonNodeToTemporalCodec<LocalDate> {

  private final ZoneId timeZone;

  public JsonNodeToLocalDateCodec(
      TemporalFormat parser, ZoneId timeZone, List<String> nullStrings) {
    super(TypeCodecs.DATE, parser, nullStrings);
    this.timeZone = timeZone;
  }

  @Override
  public LocalDate externalToInternal(JsonNode node) {
    TemporalAccessor temporal = parseTemporalAccessor(node);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toLocalDate(temporal, timeZone);
  }
}
