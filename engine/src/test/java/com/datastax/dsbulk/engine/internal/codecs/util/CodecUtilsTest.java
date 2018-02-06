/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.convertNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.convertTemporal;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.formatNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.formatTemporal;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.numberToInstant;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseTemporal;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toBigDecimal;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toBigIntegerExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toByteValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toDoubleValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toFloatValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toInstant;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toIntValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toLocalDate;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toLocalDateTime;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toLocalTime;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toLongValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toShortValueExact;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.toZonedDateTime;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.Instant.EPOCH;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CodecUtilsTest {

  private final Instant i1 = Instant.parse("2017-11-23T12:24:59Z");
  private final Instant i2 = Instant.parse("2018-02-01T00:00:00Z");
  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");

  private final DecimalFormat numberFormat1 =
      CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN);

  private final DecimalFormat numberFormat2 =
      CodecSettings.getNumberFormat("0.###E0", US, UNNECESSARY);

  private final DateTimeFormatter timestampFormat1 =
      CodecSettings.getDateTimeFormat("CQL_DATE_TIME", UTC, US, EPOCH.atZone(UTC));

  private final DateTimeFormatter timestampFormat2 =
      CodecSettings.getDateTimeFormat(("yyyyMMddHHmmss"), ofHours(2), US, EPOCH.atZone(UTC));

  private final DateTimeFormatter localDateFormat =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_DATE", UTC, US, EPOCH.atZone(UTC));

  private final DateTimeFormatter localTimeFormat =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_TIME", UTC, US, EPOCH.atZone(UTC));

  private Map<String, Boolean> booleanInputWords = ImmutableMap.of("true", true, "false", false);

  private List<BigDecimal> booleanNumbers = Lists.newArrayList(BigDecimal.ONE, BigDecimal.ZERO);

  @SuppressWarnings("ConstantConditions")
  @Test
  void should_parse_temporal_complex() {
    assertThat(parseTemporal(null, timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)).isNull();
    assertThat(parseTemporal("", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)).isNull();
    assertThat(
            Instant.from(
                parseTemporal(
                    "2017-11-23T14:24:59.999999999+02:00",
                    timestampFormat1,
                    numberFormat1,
                    MILLISECONDS,
                    EPOCH)))
        .isEqualTo(Instant.parse("2017-11-23T12:24:59.999999999Z"));
    assertThat(
            Instant.from(
                parseTemporal(
                    "2017-11-23T14:24:59+02:00",
                    timestampFormat1,
                    numberFormat1,
                    MILLISECONDS,
                    EPOCH)))
        .isEqualTo(Instant.parse("2017-11-23T12:24:59Z"));
    assertThat(
            Instant.from(
                parseTemporal(
                    "2017-11-23T14:24+02:00",
                    timestampFormat1,
                    numberFormat1,
                    MILLISECONDS,
                    EPOCH)))
        .isEqualTo(Instant.parse("2017-11-23T12:24:00Z"));
    assertThat(
            Instant.from(
                parseTemporal(
                    "2017-11-23+02:00", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(Instant.parse("2017-11-22T22:00:00Z"));
    assertThat(
            Instant.from(
                parseTemporal("2017-11-23", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(Instant.parse("2017-11-23T00:00:00Z"));
    assertThat(
            Instant.from(
                parseTemporal(
                    "14:24:59+02:00", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(Instant.parse("1970-01-01T12:24:59Z"));
    assertThat(
            Instant.from(
                parseTemporal("14:24+02:00", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(Instant.parse("1970-01-01T12:24:00Z"));
    assertThat(
            Instant.from(
                parseTemporal("14:24", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(Instant.parse("1970-01-01T14:24:00Z"));
    assertThat(
            Instant.from(parseTemporal("0", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(EPOCH);
    assertThat(
            Instant.from(
                parseTemporal("123", timestampFormat1, numberFormat1, MILLISECONDS, EPOCH)))
        .isEqualTo(ofEpochMilli(123));
  }

  @Test
  void should_parse_temporal() {
    assertThat(parseTemporal(null, timestampFormat1)).isNull();
    assertThat(parseTemporal("", timestampFormat1)).isNull();
    assertThat(
            Instant.from(
                parseTemporal("2017-11-23T13:24:59.000+01:00[Europe/Paris]", timestampFormat1)))
        .isEqualTo(i1);
    assertThat(
            Instant.from(
                parseTemporal("2017-11-23T13:24:59+01:00[Europe/Paris]", timestampFormat1)))
        .isEqualTo(i1);
    assertThat(Instant.from(parseTemporal("2017-11-23T14:24:59+02:00", timestampFormat1)))
        .isEqualTo(i1);
    assertThat(Instant.from(parseTemporal("2017-11-23T12:24:59", timestampFormat1))).isEqualTo(i1);
    assertThat(Instant.from(parseTemporal("20171123142459", timestampFormat2))).isEqualTo(i1);
    assertThat(Instant.from(parseTemporal("2018-02-01", timestampFormat1))).isEqualTo(i2);
    assertThat(Instant.from(parseTemporal("2018-02-01T00:00", timestampFormat1))).isEqualTo(i2);
    assertThat(Instant.from(parseTemporal("2018-02-01T00:00:00", timestampFormat1))).isEqualTo(i2);
    assertThat(Instant.from(parseTemporal("2018-02-01T00:00:00Z", timestampFormat1))).isEqualTo(i2);
    assertThat(LocalDate.from(parseTemporal("2018-02-01", localDateFormat)))
        .isEqualTo(LocalDate.parse("2018-02-01"));
    assertThat(LocalTime.from(parseTemporal("13:24:59.123456789", localTimeFormat)))
        .isEqualTo(LocalTime.parse("13:24:59.123456789"));
    assertThat(LocalTime.from(parseTemporal("13:24:59", localTimeFormat)))
        .isEqualTo(LocalTime.parse("13:24:59"));
    assertThat(LocalTime.from(parseTemporal("13:24", localTimeFormat)))
        .isEqualTo(LocalTime.parse("13:24:00"));
  }

  @Test
  void should_format_temporal() {
    assertThat(formatTemporal(Instant.parse("2017-11-23T14:24:59.999Z"), timestampFormat1))
        .isEqualTo("2017-11-23T14:24:59.999Z");
    assertThat(formatTemporal(Instant.parse("2017-11-23T14:24:59.999Z"), timestampFormat2))
        .isEqualTo("20171123162459"); // at +02:00
    assertThat(formatTemporal(LocalDate.parse("2018-02-01"), localDateFormat))
        .isEqualTo("2018-02-01");
    assertThat(formatTemporal(LocalTime.parse("14:24:59.999"), localTimeFormat))
        .isEqualTo("14:24:59.999");
  }

  @Test
  void should_parse_number_complex() {
    assertThat(
            parseNumber(
                null,
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isNull();
    assertThat(
            parseNumber(
                "",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isNull();
    // user patterns
    assertThat(
            parseNumber(
                "-123456.78",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    assertThat(
            parseNumber(
                "-123,456.78",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    // exponent
    assertThat(
            parseNumber(
                "1,234.123E78",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("1234.123E78"));
    // java notation
    assertThat(
            parseNumber(
                "0x1.fffP+1023",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(0x1.fffP+1023); // parsed as double
    assertThat(
            parseNumber(
                "+Infinity",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.POSITIVE_INFINITY); // parsed as double
    assertThat(
            parseNumber(
                "-Infinity",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.NEGATIVE_INFINITY); // parsed as double
    assertThat(
            parseNumber(
                "NaN",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(Double.NaN); // parsed as double
    // timestamps -> unit since epoch
    assertThat(
            parseNumber(
                "2017-12-05T12:44:36+01:00",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli());
    // booleans
    assertThat(
            parseNumber(
                "TRUE",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(BigDecimal.ONE);
    assertThat(
            parseNumber(
                "FALSE",
                numberFormat1,
                timestampFormat1,
                MILLISECONDS,
                EPOCH.atZone(UTC),
                booleanInputWords,
                booleanNumbers))
        .isEqualTo(BigDecimal.ZERO);
  }

  @Test
  void should_parse_number() throws ParseException {
    assertThat(parseNumber("1,234.567", numberFormat1)).isEqualTo(new BigDecimal("1234.567"));
    assertThat(parseNumber("0.1234E10", numberFormat2)).isEqualTo(new BigDecimal("0.1234E10"));
    assertThatThrownBy(() -> parseNumber("1,234.567", numberFormat2))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Invalid number format: 1,234.567");
    assertThatThrownBy(() -> parseNumber("0.1234 ABC", numberFormat1))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Invalid number format: 0.1234 ABC");
  }

  @Test
  void should_format_number() {
    assertThat(formatNumber(null, numberFormat1)).isNull();
    // rounded up because of HALF_EVEN
    assertThat(formatNumber(123_456.789, numberFormat1)).isEqualTo("123,456.79");
    assertThat(formatNumber(Float.MAX_VALUE, numberFormat1))
        .isEqualTo("340,282,350,000,000,000,000,000,000,000,000,000,000");
    // rounded to 0.00, then -> 0 because fraction digits are optional
    assertThat(formatNumber(Float.MIN_VALUE, numberFormat1)).isEqualTo("0");
    assertThat(formatNumber(Float.MIN_VALUE, numberFormat2)).isEqualTo("1.4E-45");
    // special Double values
    assertThat(formatNumber(Double.NaN, numberFormat1)).isEqualTo("NaN");
    assertThat(formatNumber(Double.POSITIVE_INFINITY, numberFormat1)).isEqualTo("Infinity");
    assertThat(formatNumber(Double.NEGATIVE_INFINITY, numberFormat1)).isEqualTo("-Infinity");
    // with rounding mode UNNECESSARY, check that all fraction digits are printed
    DecimalFormat unlimited = CodecSettings.getNumberFormat("#.##", US, UNNECESSARY);
    assertThat(formatNumber(Math.PI, unlimited)).isEqualTo("3.141592653589793");
    assertThat(formatNumber(Float.MIN_VALUE, unlimited))
        .isEqualTo("0.0000000000000000000000000000000000000000000014");
  }

  @Test
  void should_convert_temporal() {
    assertThat(convertTemporal(null, LocalDate.class, UTC, LocalDate.ofEpochDay(0))).isNull();
    // to LocalDate
    assertThat(
            convertTemporal(
                Instant.parse("2010-06-30T00:00:00Z"),
                LocalDate.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // to LocalTime
    assertThat(
            convertTemporal(
                Instant.parse("1970-01-01T23:59:59Z"),
                LocalTime.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // to LocalDateTime
    assertThat(
            convertTemporal(
                Instant.parse("1970-01-01T23:59:59Z"),
                LocalDateTime.class,
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // to Instant
    assertThat(
            convertTemporal(
                LocalDate.parse("2010-06-30"), Instant.class, UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T00:00:00Z"));
    // to ZonedDateTime
    assertThat(
            convertTemporal(
                LocalDate.parse("2010-06-30"), ZonedDateTime.class, UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00Z"));
    // to unsupported temporal
    assertThatThrownBy(
            () ->
                convertTemporal(
                    ZonedDateTime.parse("2010-06-30T00:00:00Z"),
                    YearMonth.class,
                    UTC,
                    LocalDate.ofEpochDay(0)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void test_toZonedDateTime() {
    // from LocalDate
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00Z"));
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    assertThat(toZonedDateTime(LocalDate.parse("2010-06-30"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T01:00:00Z"));
    // from LocalTime
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-02T00:59:59Z"));
    assertThat(toZonedDateTime(LocalTime.parse("23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T22:59:59Z"));
    // from LocalDateTime
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T23:59:59Z"));
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-07-01T00:59:59Z"));
    assertThat(
            toZonedDateTime(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T22:59:59Z"));
    // from Instant
    assertThat(toZonedDateTime(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    assertThat(
            toZonedDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T22:59:59-01:00"));
    assertThat(
            toZonedDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-02T00:59:59+01:00"));
    // from parsed temporals
    assertThat(
            toZonedDateTime(
                parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1),
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toZonedDateTime(
                parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("2010-06-29T23:00:00Z"));
    // from ZonedDateTime
    assertThat(
            toZonedDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    // from unsupported temporal
    assertThatThrownBy(() -> toZonedDateTime(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toInstant() {
    // from LocalDate
    assertThat(toInstant(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T00:00:00Z"));
    assertThat(toInstant(LocalDate.parse("2010-06-30"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(toInstant(LocalDate.parse("2010-06-30"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T01:00:00Z"));
    // from LocalTime
    assertThat(toInstant(LocalTime.parse("23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T23:59:59Z"));
    assertThat(toInstant(LocalTime.parse("23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-02T00:59:59Z"));
    assertThat(toInstant(LocalTime.parse("23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T22:59:59Z"));
    // from LocalDateTime
    assertThat(toInstant(LocalDateTime.parse("2010-06-30T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T23:59:59Z"));
    assertThat(
            toInstant(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-07-01T00:59:59Z"));
    assertThat(
            toInstant(
                LocalDateTime.parse("2010-06-30T23:59:59"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-30T22:59:59Z"));
    // from Instant
    assertThat(toInstant(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("1970-01-01T23:59:59Z"));
    // from ZonedDateTime
    assertThat(
            toInstant(
                ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toInstant(
                ZonedDateTime.parse("2010-06-30T00:00:00+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    // from parsed temporals
    assertThat(
            toInstant(
                parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1),
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    assertThat(
            toInstant(
                parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(Instant.parse("2010-06-29T23:00:00Z"));
    // from unsupported temporal
    assertThatThrownBy(() -> toInstant(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalDateTime() {
    // from LocalDate
    assertThat(toLocalDateTime(LocalDate.parse("2010-06-30"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("2010-06-30T00:00:00"));
    // from LocalTime
    assertThat(toLocalDateTime(LocalTime.parse("00:00:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T00:00:00"));
    // from LocalDateTime
    assertThat(
            toLocalDateTime(
                LocalDateTime.parse("1970-01-01T23:59:59"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from Instant
    assertThat(toLocalDateTime(Instant.parse("1970-01-01T23:59:59Z"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1), LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T22:59:59"));
    assertThat(
            toLocalDateTime(
                Instant.parse("1970-01-01T23:59:59Z"), ofHours(1), LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-02T00:59:59"));
    // from ZonedDateTime
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), UTC, LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                ZonedDateTime.parse("1970-01-01T23:59:59+01:00"),
                ofHours(-1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from parsed temporals
    assertThat(
            toLocalDateTime(
                parseTemporal("1970-01-01T23:59:59+01:00", timestampFormat1),
                UTC,
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    assertThat(
            toLocalDateTime(
                parseTemporal("1970-01-01T23:59:59+01:00", timestampFormat1),
                ofHours(1),
                LocalDate.ofEpochDay(0)))
        .isEqualTo(LocalDateTime.parse("1970-01-01T23:59:59"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalDateTime(YearMonth.of(2018, 2), UTC, LocalDate.ofEpochDay(0)))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalDate() {
    // from LocalTime (not supported)
    assertThatThrownBy(() -> toLocalDate(LocalTime.parse("23:59:59"), UTC))
        .isInstanceOf(DateTimeException.class);
    // from LocalDate
    assertThat(toLocalDate(LocalDate.parse("2010-06-30"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from LocalDateTime
    assertThat(toLocalDate(LocalDateTime.parse("2010-06-30T00:00:00"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from Instant
    assertThat(toLocalDate(Instant.parse("2010-06-30T00:00:00Z"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(Instant.parse("2010-06-30T00:00:00Z"), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-29"));
    assertThat(toLocalDate(Instant.parse("2010-06-30T23:59:59Z"), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-07-01"));
    // from ZonedDateTime
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(toLocalDate(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from parsed temporals
    assertThat(toLocalDate(parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1), UTC))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(
            toLocalDate(parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1), ofHours(1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    assertThat(
            toLocalDate(parseTemporal("2010-06-30T00:00:00+01:00", timestampFormat1), ofHours(-1)))
        .isEqualTo(LocalDate.parse("2010-06-30"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalDate(YearMonth.of(2018, 2), UTC))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void test_toLocalTime() {
    // from LocalDate (not supported)
    assertThatThrownBy(() -> toLocalTime(LocalDate.parse("2018-02-03"), UTC))
        .isInstanceOf(DateTimeException.class);
    // from LocalTime
    assertThat(toLocalTime(LocalTime.parse("23:59:59"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from LocalDateTime
    assertThat(toLocalTime(LocalDateTime.parse("1970-01-01T23:59:59"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from Instant
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), ofHours(-1)))
        .isEqualTo(LocalTime.parse("22:59:59"));
    assertThat(toLocalTime(Instant.parse("1970-01-01T23:59:59Z"), ofHours(1)))
        .isEqualTo(LocalTime.parse("00:59:59"));
    // from ZonedDateTime
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), ofHours(1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(toLocalTime(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"), ofHours(-1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from parsed temporals
    assertThat(toLocalTime(parseTemporal("1970-01-01T23:59:59+01:00", timestampFormat1), UTC))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(
            toLocalTime(parseTemporal("1970-01-01T23:59:59+01:00", timestampFormat1), ofHours(1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    assertThat(
            toLocalTime(parseTemporal("1970-01-01T23:59:59+01:00", timestampFormat1), ofHours(-1)))
        .isEqualTo(LocalTime.parse("23:59:59"));
    // from unsupported temporal
    assertThatThrownBy(() -> toLocalTime(YearMonth.of(2018, 2), UTC))
        .isInstanceOf(DateTimeException.class);
  }

  @Test
  void should_convert_number() {
    assertThat(convertNumber(123, Byte.class)).isEqualTo((byte) 123);
    assertThat(convertNumber(123, Short.class)).isEqualTo((short) 123);
    assertThat(convertNumber(123, Integer.class)).isEqualTo(123);
    assertThat(convertNumber(123, Long.class)).isEqualTo(123L);
    assertThat(convertNumber(123, BigInteger.class)).isEqualTo(new BigInteger("123"));
    assertThat(convertNumber(123, Float.class)).isEqualTo(123f);
    assertThat(convertNumber(123, Double.class)).isEqualTo(123d);
    assertThat(convertNumber(123, BigDecimal.class)).isEqualTo(new BigDecimal("123"));
    assertThatThrownBy(() -> convertNumber(123, AtomicLong.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot convert 123 of class java.lang.Integer to class java.util.concurrent.atomic.AtomicLong");
  }

  @Test
  void test_toByteValueExact() {
    assertThat(toByteValueExact((byte) 123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact((short) 123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123L)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123d)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(123f)).isEqualTo((byte) 123);
    assertThat(toByteValueExact(BigInteger.valueOf(123L))).isEqualTo((byte) 123);
    assertThat(toByteValueExact(BigDecimal.valueOf(123d))).isEqualTo((byte) 123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toByteValueExact(123.45f)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toByteValueExact(123.45d)).isInstanceOf(ArithmeticException.class);
    // too big for byte
    assertThatThrownBy(() -> toByteValueExact(Short.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toByteValueExact(Integer.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toByteValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toByteValueExact(BigInteger.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toByteValueExact(BigDecimal.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toShortValueExact() {
    assertThat(toShortValueExact((byte) 123)).isEqualTo((short) 123);
    assertThat(toShortValueExact((short) 123)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123L)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123d)).isEqualTo((short) 123);
    assertThat(toShortValueExact(123f)).isEqualTo((short) 123);
    assertThat(toShortValueExact(BigInteger.valueOf(123L))).isEqualTo((short) 123);
    assertThat(toShortValueExact(BigDecimal.valueOf(123d))).isEqualTo((short) 123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toShortValueExact(123.45f)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toShortValueExact(123.45d)).isInstanceOf(ArithmeticException.class);
    // too big for short
    assertThatThrownBy(() -> toShortValueExact(Integer.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toShortValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toShortValueExact(BigInteger.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toShortValueExact(BigDecimal.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toIntValueExact() {
    assertThat(toIntValueExact((byte) 123)).isEqualTo(123);
    assertThat(toIntValueExact((short) 123)).isEqualTo(123);
    assertThat(toIntValueExact(123)).isEqualTo(123);
    assertThat(toIntValueExact(123L)).isEqualTo(123);
    assertThat(toIntValueExact(123d)).isEqualTo(123);
    assertThat(toIntValueExact(123f)).isEqualTo(123);
    assertThat(toIntValueExact(BigInteger.valueOf(123L))).isEqualTo(123);
    assertThat(toIntValueExact(BigDecimal.valueOf(123d))).isEqualTo(123);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toIntValueExact(123.45f)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toIntValueExact(123.45d)).isInstanceOf(ArithmeticException.class);
    // too big for int
    assertThatThrownBy(() -> toIntValueExact(Long.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toIntValueExact(BigInteger.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toIntValueExact(BigDecimal.valueOf(Long.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toLongValueExact() {
    assertThat(toLongValueExact((byte) 123)).isEqualTo(123L);
    assertThat(toLongValueExact((short) 123)).isEqualTo(123L);
    assertThat(toLongValueExact(123)).isEqualTo(123L);
    assertThat(toLongValueExact(123L)).isEqualTo(123L);
    assertThat(toLongValueExact(123d)).isEqualTo(123L);
    assertThat(toLongValueExact(123f)).isEqualTo(123L);
    assertThat(toLongValueExact(BigInteger.valueOf(123L))).isEqualTo(123L);
    assertThat(toLongValueExact(BigDecimal.valueOf(123d))).isEqualTo(123L);
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toLongValueExact(123.45f)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toLongValueExact(123.45d)).isInstanceOf(ArithmeticException.class);
    // too big for long
    assertThatThrownBy(
            () -> toLongValueExact(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(
            () -> toLongValueExact(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toBigIntegerExact() {
    assertThat(toBigIntegerExact((byte) 123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact((short) 123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123L)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123d)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(123f)).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(BigInteger.valueOf(123L))).isEqualTo(BigInteger.valueOf(123L));
    assertThat(toBigIntegerExact(BigDecimal.valueOf(123d))).isEqualTo(BigInteger.valueOf(123L));
    // decimal -> integral conversions should fail
    assertThatThrownBy(() -> toBigIntegerExact(123.45f)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toBigIntegerExact(123.45d)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> toBigIntegerExact(BigDecimal.valueOf(123.45d)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toFloatValueExact() {
    assertThat(toFloatValueExact((byte) 123)).isEqualTo(123f);
    assertThat(toFloatValueExact((short) 123)).isEqualTo(123f);
    assertThat(toFloatValueExact(123)).isEqualTo(123f);
    assertThat(toFloatValueExact(123L)).isEqualTo(123f);
    assertThat(toFloatValueExact(123d)).isEqualTo(123f);
    assertThat(toFloatValueExact(123f)).isEqualTo(123f);
    assertThat(toFloatValueExact(BigInteger.valueOf(123L))).isEqualTo(123f);
    assertThat(toFloatValueExact(BigDecimal.valueOf(123d))).isEqualTo(123f);
    // float -> double type widening may alter the original
    assertThatThrownBy(() -> toFloatValueExact((double) Float.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    // too big for float
    assertThatThrownBy(() -> toFloatValueExact(Double.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class);
    // too many significant digits
    assertThatThrownBy(() -> toFloatValueExact(0.1234567891234d))
        .isInstanceOf(ArithmeticException.class);
    // too big for float
    assertThatThrownBy(() -> toFloatValueExact(BigDecimal.valueOf(Double.MAX_VALUE)))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toDoubleValueExact() {
    assertThat(toDoubleValueExact((byte) 123)).isEqualTo(123d);
    assertThat(toDoubleValueExact((short) 123)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123L)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123d)).isEqualTo(123d);
    assertThat(toDoubleValueExact(123f)).isEqualTo(123d);
    assertThat(toDoubleValueExact(BigInteger.valueOf(123L))).isEqualTo(123d);
    assertThat(toDoubleValueExact(BigDecimal.valueOf(123d))).isEqualTo(123d);
    // too big for double
    assertThatThrownBy(
            () -> toDoubleValueExact(BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE)))
        .isInstanceOf(ArithmeticException.class);
    // too many significant digits
    assertThatThrownBy(() -> toDoubleValueExact(new BigDecimal("0.1234567890123456789")))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  void test_toBigDecimal() {
    assertThat(toBigDecimal((byte) 123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal((short) 123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123L)).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(123d)).isEqualTo(new BigDecimal("123.0"));
    assertThat(toBigDecimal(123f)).isEqualTo(new BigDecimal("123.0"));
    assertThat(toBigDecimal(BigInteger.valueOf(123L))).isEqualTo(new BigDecimal("123"));
    assertThat(toBigDecimal(new BigDecimal("123.0"))).isEqualTo(new BigDecimal("123.0"));
  }

  @Test
  void should_convert_number_to_instant() {
    assertThat(numberToInstant(null, MILLISECONDS, EPOCH)).isNull();

    assertThat(numberToInstant(0, MILLISECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(123));
    assertThat(numberToInstant(-123, MILLISECONDS, EPOCH)).isEqualTo(ofEpochMilli(-123));
    assertThat(numberToInstant(-123, MILLISECONDS, ofEpochMilli(123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, MILLISECONDS, ofEpochMilli(-123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, SECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, SECONDS, EPOCH)).isEqualTo(ofEpochSecond(123));
    assertThat(numberToInstant(-123, SECONDS, EPOCH)).isEqualTo(ofEpochSecond(-123));
    assertThat(numberToInstant(-123, SECONDS, ofEpochSecond(123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, SECONDS, ofEpochSecond(-123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, 123));
    assertThat(numberToInstant(-123, NANOSECONDS, EPOCH)).isEqualTo(ofEpochSecond(0, -123));
    assertThat(numberToInstant(-123, NANOSECONDS, ofEpochSecond(0, 123))).isEqualTo(EPOCH);
    assertThat(numberToInstant(123, NANOSECONDS, ofEpochSecond(0, -123))).isEqualTo(EPOCH);

    assertThat(numberToInstant(0, NANOSECONDS, EPOCH)).isEqualTo(EPOCH);
    assertThat(numberToInstant(123000000456L, NANOSECONDS, EPOCH))
        .isEqualTo(ofEpochSecond(123, 456));
    assertThat(numberToInstant(-123000000456L, NANOSECONDS, EPOCH))
        .isEqualTo(ofEpochSecond(-123, -456));
    assertThat(numberToInstant(-123000000456L, NANOSECONDS, ofEpochSecond(123, 456)))
        .isEqualTo(EPOCH);
    assertThat(numberToInstant(123000000456L, NANOSECONDS, ofEpochSecond(-123, -456)))
        .isEqualTo(EPOCH);
  }

  @Test
  void should_convert_instant_to_number() {
    assertThat(instantToNumber(i1, MILLISECONDS, EPOCH)).isEqualTo(i1.toEpochMilli());
    assertThat(instantToNumber(i1, NANOSECONDS, EPOCH)).isEqualTo(i1.toEpochMilli() * 1_000_000);
    assertThat(instantToNumber(i1, SECONDS, EPOCH)).isEqualTo(i1.getEpochSecond());
    assertThat(instantToNumber(i1, MINUTES, EPOCH)).isEqualTo(i1.getEpochSecond() / 60);

    assertThat(instantToNumber(i1, MILLISECONDS, millennium))
        .isEqualTo(i1.toEpochMilli() - millennium.toEpochMilli());
    assertThat(instantToNumber(i1, NANOSECONDS, millennium))
        .isEqualTo(i1.toEpochMilli() * 1_000_000 - millennium.toEpochMilli() * 1_000_000);
    assertThat(instantToNumber(i1, SECONDS, millennium))
        .isEqualTo(i1.getEpochSecond() - millennium.getEpochSecond());
    assertThat(instantToNumber(i1, MINUTES, millennium))
        .isEqualTo(i1.getEpochSecond() / 60 - millennium.getEpochSecond() / 60);
  }

  @Test
  void should_parse_uuid() {
    ThreadLocal<DecimalFormat> numberFormat1 = ThreadLocal.withInitial(() -> this.numberFormat1);
    StringToInstantCodec instantCodec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat1, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(CodecUtils.parseUUID(null, instantCodec, null)).isNull();
    assertThat(CodecUtils.parseUUID("", instantCodec, null)).isNull();
    assertThat(CodecUtils.parseUUID("a15341ec-ebef-4eab-b91d-ff16bf801a79", instantCodec, null))
        .isEqualTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"));
    // time UUIDs with MIN strategy
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, MIN))
        .isEqualTo(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(CodecUtils.parseUUID("123456", instantCodec, MIN)).isEqualTo(UUIDs.startOf(123456L));
    // time UUIDs with MAX strategy
    // the driver's endOf method takes milliseconds and sets all the sub-millisecond digits to their
    // max, that's why we add .000999999
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36.000999999+01:00", instantCodec, MAX))
        .isEqualTo(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(CodecUtils.parseUUID("123456", instantCodec, MAX).timestamp())
        .isEqualTo(UUIDs.endOf(123456L).timestamp() - 9999);
    // time UUIDs with FIXED strategy
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, FIXED).timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(CodecUtils.parseUUID("123456", instantCodec, FIXED).timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    // time UUIDs with RANDOM strategy
    assertThat(CodecUtils.parseUUID("2017-12-05T12:44:36+01:00", instantCodec, RANDOM).timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(CodecUtils.parseUUID("123456", instantCodec, RANDOM).timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    // invalid UUIDs
    assertThrows(
        IllegalArgumentException.class,
        () -> CodecUtils.parseUUID("not a valid UUID", instantCodec, MIN));
  }

  @Test
  void should_parse_byte_buffer() {
    byte[] data = {1, 2, 3, 4, 5, 6};
    String data64 = Base64.getEncoder().encodeToString(data);
    String dataHex = Bytes.toHexString(data);
    assertThat(CodecUtils.parseByteBuffer(null)).isNull();
    assertThat(CodecUtils.parseByteBuffer("")).isNull();
    assertThat(CodecUtils.parseByteBuffer("0x")).isEqualTo(ByteBuffer.wrap(new byte[] {}));
    assertThat(CodecUtils.parseByteBuffer(data64)).isEqualTo(ByteBuffer.wrap(data));
    assertThat(CodecUtils.parseByteBuffer(dataHex)).isEqualTo(ByteBuffer.wrap(data));
  }
}
