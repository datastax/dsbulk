/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  private final TimeUnit numericTimestampUnit;
  private final Instant numericTimestampEpoch;

  public StringToInstantCodec(
      DateTimeFormatter parser, TimeUnit numericTimestampUnit, Instant numericTimestampEpoch) {
    super(InstantCodec.instance, parser);
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
  }

  @Override
  public Instant convertFrom(String s) {
    TemporalAccessor temporal =
        CodecUtils.parseTemporal(s, parser, numericTimestampUnit, numericTimestampEpoch);
    if (temporal == null) {
      return null;
    }
    try {
      return Instant.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse instant:" + s, e);
    }
  }
}
