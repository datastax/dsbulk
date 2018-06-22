/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import org.junit.jupiter.api.Test;

class DateToUUIDCodecTest {

  private DateToTemporalCodec<Date, Instant> instantCodec =
      new DateToTemporalCodec<>(Date.class, TypeCodecs.TIMESTAMP, ZoneOffset.UTC);

  @Test
  void should_convert_when_valid_input() {

    assertThat(new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .externalToInternal(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .externalToInternal(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .externalToInternal(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .externalToInternal(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));
  }
}
