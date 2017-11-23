/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  public StringToInstantCodec(DateTimeFormatter parser) {
    super(InstantCodec.instance, parser);
  }

  @Override
  public Instant convertFrom(String s) {
    TemporalAccessor temporal = parseAsTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    try {
      return ZonedDateTime.from(temporal).toInstant();
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse instant:" + s, e);
    }
  }
}
