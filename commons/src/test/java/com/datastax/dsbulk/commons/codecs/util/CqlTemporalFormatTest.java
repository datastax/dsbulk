/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.util;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class CqlTemporalFormatTest {

  @ParameterizedTest(name = "\"{0}\" with zone {2} should parse to {1}")
  @ArgumentsSource(Parse.class)
  void should_parse_from_valid_cql_literal(String input, TemporalAccessor expected, ZoneId zone) {
    CqlTemporalFormat format = new CqlTemporalFormat(zone);
    TemporalAccessor actual = format.parse(input);
    long actualSeconds = toSeconds(actual);
    long expectedSeconds = toSeconds(expected);
    assertThat(actualSeconds)
        .overridingErrorMessage(
            "Expecting %s to be same instant as %s but it was not", actual, expected)
        .isEqualTo(expectedSeconds);
  }

  @ParameterizedTest(name = "{0} with zone {2} should format to \"{1}\"")
  @ArgumentsSource(Format.class)
  void should_format_to_valid_cql_literal(TemporalAccessor input, String expected, ZoneId zone) {
    CqlTemporalFormat format = new CqlTemporalFormat(zone);
    String actual = format.format(input);
    assertThat(actual).isEqualTo(expected);
  }

  // all valid CQL patterns, as used in Cassandra 2.2+
  private static final List<String> PATTERNS =
      Lists.newArrayList(
          "yyyy-MM-dd HH:mm",
          "yyyy-MM-dd HH:mm:ss",
          "yyyy-MM-dd HH:mm z",
          "yyyy-MM-dd HH:mm zz",
          "yyyy-MM-dd HH:mm zzz",
          "yyyy-MM-dd HH:mmX",
          "yyyy-MM-dd HH:mmXX",
          "yyyy-MM-dd HH:mmXXX",
          "yyyy-MM-dd HH:mm:ss",
          "yyyy-MM-dd HH:mm:ss z",
          "yyyy-MM-dd HH:mm:ss zz",
          "yyyy-MM-dd HH:mm:ss zzz",
          "yyyy-MM-dd HH:mm:ssX",
          "yyyy-MM-dd HH:mm:ssXX",
          "yyyy-MM-dd HH:mm:ssXXX",
          "yyyy-MM-dd HH:mm:ss.SSS",
          "yyyy-MM-dd HH:mm:ss.SSS z",
          "yyyy-MM-dd HH:mm:ss.SSS zz",
          "yyyy-MM-dd HH:mm:ss.SSS zzz",
          "yyyy-MM-dd HH:mm:ss.SSSX",
          "yyyy-MM-dd HH:mm:ss.SSSXX",
          "yyyy-MM-dd HH:mm:ss.SSSXXX",
          "yyyy-MM-dd'T'HH:mm",
          "yyyy-MM-dd'T'HH:mm z",
          "yyyy-MM-dd'T'HH:mm zz",
          "yyyy-MM-dd'T'HH:mm zzz",
          "yyyy-MM-dd'T'HH:mmX",
          "yyyy-MM-dd'T'HH:mmXX",
          "yyyy-MM-dd'T'HH:mmXXX",
          "yyyy-MM-dd'T'HH:mm:ss",
          "yyyy-MM-dd'T'HH:mm:ss z",
          "yyyy-MM-dd'T'HH:mm:ss zz",
          "yyyy-MM-dd'T'HH:mm:ss zzz",
          "yyyy-MM-dd'T'HH:mm:ssX",
          "yyyy-MM-dd'T'HH:mm:ssXX",
          "yyyy-MM-dd'T'HH:mm:ssXXX",
          "yyyy-MM-dd'T'HH:mm:ss.SSS",
          "yyyy-MM-dd'T'HH:mm:ss.SSS z",
          "yyyy-MM-dd'T'HH:mm:ss.SSS zz",
          "yyyy-MM-dd'T'HH:mm:ss.SSS zzz",
          "yyyy-MM-dd'T'HH:mm:ss.SSSX",
          "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
          "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
          "yyyy-MM-dd",
          "yyyy-MM-dd z",
          "yyyy-MM-dd zz",
          "yyyy-MM-dd zzz",
          "yyyy-MM-ddX",
          "yyyy-MM-ddXX",
          "yyyy-MM-ddXXX");

  private static final List<ZoneId> ZONES =
      Lists.newArrayList(
          ZoneId.of("UTC"),
          ZoneId.of("Z"),
          ZoneId.of("GMT"),
          ZoneId.of("GMT+2"),
          ZoneId.of("UTC+01:00"),
          ZoneId.of("+02:00"),
          ZoneId.of("PST", ZoneId.SHORT_IDS),
          ZoneId.of("CET", ZoneId.SHORT_IDS),
          ZoneId.of("Europe/Paris"),
          ZoneId.of("-08"),
          ZoneId.of("-0830"),
          ZoneId.of("-083015"));

  private static long toSeconds(TemporalAccessor actual) {
    if (actual instanceof ZonedDateTime) {
      return ((ZonedDateTime) actual).toEpochSecond();
    } else {
      return actual.getLong(INSTANT_SECONDS);
    }
  }

  private static class Parse implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      List<Arguments> args = new ArrayList<>();
      for (ZoneId zone : ZONES) {
        ZonedDateTime zdt = Instant.parse("2019-08-01T12:34:56Z").atZone(zone);
        for (String pattern : PATTERNS) {
          if (checkZone(pattern, zone)) {
            DateTimeFormatter f = createFormatter(pattern, zone);
            String input = f.format(zdt);
            // f has an overriding time zone, but it's ok since it's the same one in input
            TemporalAccessor expected = f.parse(input);
            args.add(Arguments.of(input, expected, zone));
          }
        }
      }
      return args.stream();
    }

    @NonNull
    private static DateTimeFormatter createFormatter(String pattern, ZoneId zone) {
      return new DateTimeFormatterBuilder()
          .appendPattern(pattern)
          .parseDefaulting(HOUR_OF_DAY, 0)
          .parseDefaulting(MINUTE_OF_HOUR, 0)
          .parseDefaulting(SECOND_OF_MINUTE, 0)
          .parseDefaulting(NANO_OF_SECOND, 0)
          .toFormatter()
          .withZone(zone);
    }

    private static boolean checkZone(String pattern, ZoneId zone) {
      if (pattern.contains("X") && zone instanceof ZoneOffset) {
        int offset = ((ZoneOffset) zone).getTotalSeconds();
        int rounded = (offset / 60) * 60;
        // X patterns cannot print offsets including seconds
        return offset == rounded;
      }
      return true;
    }
  }

  private static class Format implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      List<Arguments> args = new ArrayList<>();
      for (ZoneId zone : ZONES) {
        ZonedDateTime zdt = Instant.parse("2019-08-01T12:34:56Z").atZone(zone);
        DateTimeFormatter f = ISO_OFFSET_DATE_TIME.withZone(zone);
        args.add(Arguments.of(zdt, f.format(zdt), zone));
      }
      return args.stream();
    }
  }
}
