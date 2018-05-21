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
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import org.junit.jupiter.api.Test;

class CqlTemporalFormatTest {

  private final Instant i1 = Instant.parse("2017-11-23T12:24:59Z");
  private final Instant i2 = Instant.parse("2018-02-01T00:00:00Z");

  private final TemporalFormat format = CqlTemporalFormat.DEFAULT_INSTANCE;

  // all valid CQL patterns, as used in Cassandra 2.2+
  private final List<String> patterns =
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

  private final List<ZoneId> zones =
      Lists.newArrayList(
          ZoneId.of("UTC"),
          ZoneId.of("Z"),
          ZoneId.of("GMT"),
          ZoneId.of("GMT+2"),
          ZoneId.of("UTC+01:00"),
          ZoneId.of("+02:00"),
          ZoneId.of("PST", ZoneId.SHORT_IDS),
          ZoneId.of("CET", ZoneId.SHORT_IDS),
          ZoneId.of("-08"),
          ZoneId.of("-0830"),
          ZoneId.of("-08:30"),
          ZoneId.of("-083015"),
          ZoneId.of("-08:30:15"));

  @Test
  void should_parse_temporal() {
    assertThat(format.parse(null)).isNull();
    assertThat(Instant.from(format.parse("2017-11-23T13:24:59.000 CEST"))).isEqualTo(i1);
    assertThat(Instant.from(format.parse("2017-11-23T13:24:59 CEST"))).isEqualTo(i1);
    assertThat(Instant.from(format.parse("2017-11-23T14:24:59+02:00"))).isEqualTo(i1);
    assertThat(Instant.from(format.parse("2017-11-23T12:24:59"))).isEqualTo(i1);
    assertThat(Instant.from(format.parse("2018-02-01"))).isEqualTo(i2);
    assertThat(Instant.from(format.parse("2018-02-01T00:00"))).isEqualTo(i2);
    assertThat(Instant.from(format.parse("2018-02-01T00:00:00"))).isEqualTo(i2);
    assertThat(Instant.from(format.parse("2018-02-01T00:00:00Z"))).isEqualTo(i2);
  }

  @Test
  void should_format_temporal() {
    assertThat(format.format(Instant.parse("2017-11-23T14:24:59.999Z")))
        .isEqualTo("2017-11-23 14:24:59.999 UTC");
  }

  @Test
  void should_parse_all_valid_cql_literals() {
    for (String pattern : patterns) {
      for (ZoneId zone : zones) {
        boolean zoned = pattern.contains("X") || pattern.contains("z");
        if (zoned && zone instanceof ZoneOffset) {
          int offset = ((ZoneOffset) zone).getTotalSeconds();
          int normalized = (offset / 60) * 60;
          // offsets including minutes cannot be properly parsed/formatted with valid CQL formats
          if (offset != normalized) {
            continue;
          }
        }
        CqlTemporalFormat format = new CqlTemporalFormat(zone, US);
        DateTimeFormatter f =
            new DateTimeFormatterBuilder()
                .parseStrict()
                .parseCaseInsensitive()
                .appendPattern(pattern)
                .parseDefaulting(HOUR_OF_DAY, 0)
                .parseDefaulting(MINUTE_OF_HOUR, 0)
                .parseDefaulting(SECOND_OF_MINUTE, 0)
                .parseDefaulting(NANO_OF_SECOND, 0)
                .toFormatter(US)
                .withZone(zone);
        String input = f.format(i1);
        TemporalAccessor expected = f.parse(input);
        TemporalAccessor actual = format.parse(input);
        long actualSeconds;
        if (actual instanceof ZonedDateTime) {
          actualSeconds = ((ZonedDateTime) actual).toEpochSecond();
        } else {
          actualSeconds = actual.getLong(ChronoField.INSTANT_SECONDS);
        }
        long expectedSeconds = expected.getLong(ChronoField.INSTANT_SECONDS);
        assertThat(actualSeconds)
            .overridingErrorMessage(
                "Expecting %s to be same instant as %s but it was not", actual, i1)
            .isEqualTo(expectedSeconds);
      }
    }
  }

  @Test
  void should_format_all_valid_cql_literals() {
    for (ZoneId zone : zones) {
      DateTimeFormatter f =
          DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS zzz").withZone(zone);
      CqlTemporalFormat format = new CqlTemporalFormat(zone, US);
      String actual = format.format(i1);
      String expected = f.format(i1);
      assertThat(actual).isEqualTo(expected);
    }
  }
}
