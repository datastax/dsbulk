/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class NumberToUUIDCodecTest {

  private NumberToInstantCodec<Long> instantCodec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    CommonsAssertions.assertThat(new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());
  }
}
