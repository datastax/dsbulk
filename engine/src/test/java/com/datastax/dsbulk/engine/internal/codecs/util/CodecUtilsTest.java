/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseInstant;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.parseTemporal;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** */
class CodecUtilsTest {

  private final Instant i = parse("2017-11-23T12:24:59Z");
  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");

  @SuppressWarnings("ConstantConditions")
  @Test
  void should_parse_temporal() {
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
  void should_parse_alpha_numeric_temporal() {
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
  void should_parse_numeric_temporal() {
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
  void should_convert_temporal_to_timestamp_since_epoch() {
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

  @Test
  void should_parse_uuid() {
    StringToInstantCodec instantCodec =
        new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
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
    // the driver's endOf method takes milliseconds and sets all the sub-millisecond digits to their max,
    // that's why we add .000999999
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
        InvalidTypeException.class,
        () -> CodecUtils.parseUUID("not a valid UUID", instantCodec, MIN));
  }

  @Test
  void should_parse_number() {
    DecimalFormat formatter =
        new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US));
    formatter.setParseBigDecimal(true);
    Map<String, Boolean> booleanWords = ImmutableMap.of("true", true, "false", false);
    List<BigDecimal> booleanNumbers = newArrayList(ONE, ZERO);
    assertThat(
            CodecUtils.parseNumber(
                null,
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isNull();
    assertThat(
            CodecUtils.parseNumber(
                "",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isNull();
    assertThat(
            CodecUtils.parseNumber(
                "-123456.78",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    assertThat(
            CodecUtils.parseNumber(
                "-123,456.78",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isEqualTo(new BigDecimal("-123456.78"));
    assertThat(
            CodecUtils.parseNumber(
                "2017-12-05T12:44:36+01:00",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isEqualTo(ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli());
    assertThat(
            CodecUtils.parseNumber(
                "TRUE",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isEqualTo(ONE);
    assertThat(
            CodecUtils.parseNumber(
                "FALSE",
                formatter,
                CQL_DATE_TIME_FORMAT,
                MILLISECONDS,
                EPOCH,
                booleanWords,
                booleanNumbers))
        .isEqualTo(ZERO);
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
