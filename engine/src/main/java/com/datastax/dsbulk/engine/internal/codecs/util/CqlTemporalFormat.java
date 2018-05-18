/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.Locale.US;

import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import org.jetbrains.annotations.NotNull;

/**
 * A special zoned temporal format that recognizes all valid CQL input formats when parsing.
 *
 * <p>When formatting, this format uses "uuuu-MM-dd HH:mm:ss.SSS zzz" as the formatting pattern,
 * thus producing output such as "2017-11-23 14:24:59.999 UTC".
 */
public class CqlTemporalFormat extends ZonedTemporalFormat {

  public static final CqlTemporalFormat DEFAULT_INSTANCE =
      new CqlTemporalFormat(ZoneId.of("UTC"), US);

  public CqlTemporalFormat(ZoneId timeZone, Locale locale) {
    super(createParser(locale), createFormatter(timeZone, locale), timeZone);
  }

  @NotNull
  private static DateTimeFormatter createParser(Locale locale) {
    // this formatter is a hybrid parser that combines all valid CQL patterns declared in C* 2.2+
    // into a single parser. To achieve that we "cheat" a little bit and accept many optional
    // components
    // that would not make sense together. For example, we accept both 'T' and blank as date-time
    // separators,
    // so in theory we also accept "T " (i.e. 'T' followed by a blank).
    return new DateTimeFormatterBuilder()
        .parseStrict()
        .parseCaseInsensitive()

        // date part
        .optionalStart()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalEnd()

        // date-time separators
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()

        // time part, includes fraction of second
        .optionalStart()
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalEnd()

        // time zone separator
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()

        // time zone part
        .optionalStart()
        .appendPattern("XXX")
        .optionalEnd()
        .optionalStart()
        .appendPattern("zzz")
        .optionalEnd()
        .optionalStart()
        .appendPattern("XX")
        .optionalEnd()
        .optionalStart()
        .appendPattern("zz")
        .optionalEnd()
        .optionalStart()
        .appendPattern("X")
        .optionalEnd()
        .optionalStart()
        .appendPattern("z")
        .optionalEnd()

        // add defaults for missing fields, which allows the parsing to be lenient when
        // some fields cannot be inferred from the input.
        .parseDefaulting(HOUR_OF_DAY, 0)
        .parseDefaulting(MINUTE_OF_HOUR, 0)
        .parseDefaulting(SECOND_OF_MINUTE, 0)
        .parseDefaulting(NANO_OF_SECOND, 0)
        .toFormatter(locale)
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE);
  }

  @NotNull
  private static DateTimeFormatter createFormatter(ZoneId timeZone, Locale locale) {
    return DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS zzz", locale)
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE)
        .withZone(timeZone);
  }
}
