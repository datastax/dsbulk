/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseInstant;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseTemporal;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.Instant.parse;
import static java.time.ZoneOffset.ofHours;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/** */
class CodecUtilsTest {

  private final Instant i = parse("2017-11-23T12:24:59Z");
  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");

  @SuppressWarnings("ConstantConditions")
  @Test
  void should_parse_temporal() throws Exception {
    assertThat(parseTemporal(null, CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH)).isNull();
    assertThat(parseTemporal("", CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH)).isNull();
    assertThat(
            Instant.from(
                parseTemporal(
                    "2017-11-23T14:24:59+02:00", CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH)))
        .isEqualTo(i);
    assertThat(
            Instant.from(
                parseTemporal(
                    "20171123142459",
                    ofPattern("yyyyMMddHHmmss").withZone(ofHours(2)),
                    MILLISECONDS,
                    EPOCH)))
        .isEqualTo(i);
    assertThat(Instant.from(parseTemporal("0", CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH)))
        .isEqualTo(EPOCH);
    assertThat(Instant.from(parseTemporal("123", CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH)))
        .isEqualTo(ofEpochMilli(123));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void should_parse_alpha_numeric_temporal() throws Exception {
    assertThat(CodecUtils.parseTemporal(null, CQL_DATE_TIME_FORMAT)).isNull();
    assertThat(CodecUtils.parseTemporal("", CQL_DATE_TIME_FORMAT)).isNull();
    assertThat(
            Instant.from(
                CodecUtils.parseTemporal("2017-11-23T14:24:59+02:00", CQL_DATE_TIME_FORMAT)))
        .isEqualTo(i);
    assertThat(Instant.from(CodecUtils.parseTemporal("2017-11-23T12:24:59", CQL_DATE_TIME_FORMAT)))
        .isEqualTo(i);
    assertThat(
            Instant.from(
                CodecUtils.parseTemporal(
                    "20171123142459", ofPattern("yyyyMMddHHmmss").withZone(ofHours(2)))))
        .isEqualTo(i);
  }

  @Test
  void should_parse_numeric_temporal() throws Exception {
    assertThat(parseInstant(null, MILLISECONDS, EPOCH)).isNull();
    assertThat(parseInstant("", MILLISECONDS, EPOCH)).isNull();

    assertThat(parseInstant("0", MILLISECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(parseInstant("123", MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(123));
    assertThat(parseInstant("-123", MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(-123));
    assertThat(parseInstant("-123", MILLISECONDS, ofEpochMilli(123))).isEqualTo(EPOCH);
    assertThat(parseInstant("123", MILLISECONDS, ofEpochMilli(-123))).isEqualTo(EPOCH);

    assertThat(parseInstant("0", SECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(parseInstant("123", SECONDS, EPOCH)).isEqualTo(ofEpochSecond(123));
    assertThat(parseInstant("-123", SECONDS, EPOCH)).isEqualTo(ofEpochSecond(-123));
    assertThat(parseInstant("-123", SECONDS, ofEpochSecond(123))).isEqualTo(EPOCH);
    assertThat(parseInstant("123", SECONDS, ofEpochSecond(-123))).isEqualTo(EPOCH);

    assertThat(parseInstant("0", NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(parseInstant("123", NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, 123));
    assertThat(parseInstant("-123", NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, -123));
    assertThat(parseInstant("-123", NANOSECONDS, ofEpochSecond(0, 123))).isEqualTo(EPOCH);
    assertThat(parseInstant("123", NANOSECONDS, ofEpochSecond(0, -123))).isEqualTo(EPOCH);

    assertThat(parseInstant("0", NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(parseInstant("123000000456", NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(123, 456));
    assertThat(parseInstant("-123000000456", NANOSECONDS, EPOCH))
        .isEqualTo(ofEpochSecond(-123, -456));
    assertThat(parseInstant("-123000000456", NANOSECONDS, ofEpochSecond(123, 456)))
        .isEqualTo(EPOCH);
    assertThat(parseInstant("123000000456", NANOSECONDS, ofEpochSecond(-123, -456)))
        .isEqualTo(EPOCH);
  }

  @Test
  void should_convert_temporal_to_timestamp_since_epoch() throws Exception {
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, MILLISECONDS, EPOCH))
        .isEqualTo(i.toEpochMilli());
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, NANOSECONDS, EPOCH))
        .isEqualTo(i.toEpochMilli() * 1_000_000);
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, SECONDS, EPOCH))
        .isEqualTo(i.getEpochSecond());
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, MINUTES, EPOCH))
        .isEqualTo(i.getEpochSecond() / 60);

    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, MILLISECONDS, millennium))
        .isEqualTo(i.toEpochMilli() - millennium.toEpochMilli());
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, NANOSECONDS, millennium))
        .isEqualTo(i.toEpochMilli() * 1_000_000 - millennium.toEpochMilli() * 1_000_000);
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, SECONDS, millennium))
        .isEqualTo(i.getEpochSecond() - millennium.getEpochSecond());
    assertThat(CodecUtils.instantToTimestampSinceEpoch(i, MINUTES, millennium))
        .isEqualTo(i.getEpochSecond() / 60 - millennium.getEpochSecond() / 60);
  }
}
