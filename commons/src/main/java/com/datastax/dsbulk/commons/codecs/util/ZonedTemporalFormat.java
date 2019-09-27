/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.util;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

/**
 * A special temporal format that applies a fixed time zone to parsed inputs when they do not
 * contain any time zone or time offset information, thus allowing such inputs to be converted to an
 * {@link Instant}.
 */
public class ZonedTemporalFormat extends SimpleTemporalFormat {

  private final ZoneId timeZone;

  public ZonedTemporalFormat(DateTimeFormatter parserAndFormatter, ZoneId timeZone) {
    this(parserAndFormatter, parserAndFormatter, timeZone);
  }

  public ZonedTemporalFormat(
      DateTimeFormatter parser, DateTimeFormatter formatter, ZoneId timeZone) {
    // Always force a time zone when formatting (makes formatting of unzoned input possible),
    // but never set a time zone when parsing (dangerous as original zone info could be overridden
    // and data altered)
    super(parser, formatter.withZone(timeZone));
    Preconditions.checkState(parser.getZone() == null, "parser should not have an override zone");
    this.timeZone = timeZone;
  }

  @Override
  public TemporalAccessor parse(String text) {
    TemporalAccessor temporal = super.parse(text);
    if (temporal == null) {
      return null;
    }
    // If an instant cannot be inferred from the input (possibly because the input did not
    // contain any time zone information), try extracting the local date/time from the input and
    // apply the default time zone.
    if (!temporal.isSupported(ChronoField.INSTANT_SECONDS)) {
      return ZonedDateTime.of(
          temporal.query(TemporalQueries.localDate()),
          temporal.query(TemporalQueries.localTime()),
          timeZone);
    } else {
      return temporal;
    }
  }
}
