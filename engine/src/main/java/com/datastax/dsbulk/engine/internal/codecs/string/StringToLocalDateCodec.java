/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class StringToLocalDateCodec extends StringToTemporalCodec<LocalDate> {

  public StringToLocalDateCodec(DateTimeFormatter parser) {
    super(LocalDateCodec.instance, parser);
  }

  @Override
  public LocalDate convertFrom(String s) {
    TemporalAccessor temporal = parseAsTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    try {
      return LocalDate.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse local date:" + s, e);
    }
  }
}
