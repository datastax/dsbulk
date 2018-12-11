/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import java.time.DateTimeException;
import java.time.temporal.TemporalAccessor;

/**
 * A small wrapper around {@link java.time.format.DateTimeFormatter} that allows to use different
 * formats when parsing and formatting.
 */
public interface TemporalFormat {

  /**
   * Parses the given string as a temporal.
   *
   * @param text the string to parse, may be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws DateTimeException if the string cannot be parsed.
   */
  TemporalAccessor parse(String text) throws DateTimeException;

  /**
   * Formats the given temporal.
   *
   * @param temporal the value to format.
   * @return the formatted value or {@code null} if the value was {@code null}.
   * @throws DateTimeException if the value cannot be formatted.
   */
  String format(TemporalAccessor temporal) throws DateTimeException;
}
