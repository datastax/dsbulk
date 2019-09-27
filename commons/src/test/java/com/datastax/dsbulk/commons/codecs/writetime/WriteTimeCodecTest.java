/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.writetime;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToInstantCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.commons.codecs.temporal.DateToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.temporal.TemporalToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.netty.util.concurrent.FastThreadLocal;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WriteTimeCodecTest {

  private final TimeUnit unit = MILLISECONDS;

  private final ZonedDateTime epoch = EPOCH.atZone(UTC);

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final TemporalFormat temporalFormat =
      CodecUtils.getTemporalFormat("CQL_TIMESTAMP", UTC, US, unit, epoch, numberFormat, true);

  private final List<String> nullStrings = newArrayList("NULL");

  @Test
  void should_convert_to_timestamp_micros() {

    assertThat(
            new WriteTimeCodec<>(new StringToInstantCodec(temporalFormat, UTC, epoch, nullStrings)))
        .convertsFromExternal("2017-11-30T14:46:56+01:00")
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));

    assertThat(
            new WriteTimeCodec<>(
                new JsonNodeToInstantCodec(temporalFormat, UTC, epoch, nullStrings)))
        .convertsFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode("2017-11-30T14:46:56+01:00"))
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));

    assertThat(
            new WriteTimeCodec<>(
                new TemporalToTemporalCodec<>(Instant.class, TypeCodecs.TIMESTAMP, UTC, epoch)))
        .convertsFromExternal(ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant())
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));

    assertThat(
            new WriteTimeCodec<>(
                new TemporalToTemporalCodec<>(
                    ZonedDateTime.class, TypeCodecs.TIMESTAMP, UTC, epoch)))
        .convertsFromExternal(ZonedDateTime.parse("2017-11-30T14:46:56+01:00"))
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));

    assertThat(
            new WriteTimeCodec<>(
                new DateToTemporalCodec<>(java.util.Date.class, TypeCodecs.TIMESTAMP, UTC)))
        .convertsFromExternal(
            Date.from(ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant()))
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));

    assertThat(
            new WriteTimeCodec<>(
                new DateToTemporalCodec<>(java.sql.Timestamp.class, TypeCodecs.TIMESTAMP, UTC)))
        .convertsFromExternal(
            Timestamp.from(ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant()))
        .toInternal(
            MILLISECONDS.toMicros(
                ZonedDateTime.parse("2017-11-30T14:46:56+01:00").toInstant().toEpochMilli()));
  }
}
