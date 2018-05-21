/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class JsonNodeToLocalTimeCodec extends JsonNodeToTemporalCodec<LocalTime> {

  public JsonNodeToLocalTimeCodec(TemporalFormat parser, List<String> nullStrings) {
    super(LocalTimeCodec.instance, parser, nullStrings);
  }

  @Override
  public LocalTime externalToInternal(JsonNode s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    try {
      return LocalTime.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse local time:" + s, e);
    }
  }
}
