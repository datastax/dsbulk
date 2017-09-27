/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class StringToLocalTimeCodec extends StringToTemporalCodec<LocalTime> {

  public StringToLocalTimeCodec(DateTimeFormatter parser) {
    super(LocalTimeCodec.instance, parser);
  }

  @Override
  public LocalTime convertFrom(String s) {
    TemporalAccessor temporal = parseAsTemporalAccessor(s);
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
