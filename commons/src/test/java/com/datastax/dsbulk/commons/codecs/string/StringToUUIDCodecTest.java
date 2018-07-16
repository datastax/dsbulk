/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StringToUUIDCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(
          CodecUtils.getTemporalFormat("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX", UTC, US),
          numberFormat,
          UTC,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          nullStrings);

  private final StringToUUIDCodec codec =
      new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, MIN, nullStrings);

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("a15341ec-ebef-4eab-b91d-ff16bf801a79")
        .toInternal(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);

    assertThat(new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, MIN, nullStrings))
        .convertsFromExternal("2017-12-05T12:44:36+01:00")
        .toInternal(
            Uuids.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, MAX, nullStrings))
        .convertsFromExternal("2017-12-05T12:44:36.999999999+01:00")
        .toInternal(
            Uuids.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, FIXED, nullStrings)
                .externalToInternal("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, RANDOM, nullStrings)
                .externalToInternal("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, MIN, nullStrings))
        .convertsFromExternal("123456")
        .toInternal(Uuids.startOf(123456L));
    assertThat(
            new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, MAX, nullStrings)
                .externalToInternal("123456")
                .timestamp())
        .isEqualTo(Uuids.startOf(123456L).timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, FIXED, nullStrings)
                .externalToInternal("123456")
                .timestamp())
        .isEqualTo(Uuids.startOf(123456L).timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodecs.UUID, instantCodec, RANDOM, nullStrings)
                .externalToInternal("123456")
                .timestamp())
        .isEqualTo(Uuids.startOf(123456L).timestamp());
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .toExternal("a15341ec-ebef-4eab-b91d-ff16bf801a79");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid UUID");
  }
}
