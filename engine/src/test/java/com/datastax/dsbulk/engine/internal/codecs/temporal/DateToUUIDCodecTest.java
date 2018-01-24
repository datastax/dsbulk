/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import static com.datastax.driver.core.TypeCodec.timeUUID;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import org.junit.jupiter.api.Test;

class DateToUUIDCodecTest {

  private DateToTemporalCodec<Date, Instant> instantCodec =
      new DateToTemporalCodec<>(Date.class, InstantCodec.instance, ZoneOffset.UTC);

  @Test
  void should_convert_when_valid_input() {

    assertThat(new DateToUUIDCodec<>(timeUUID(), instantCodec, MIN))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .convertFrom(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .convertFrom(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .convertFrom(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .convertFrom(
                    Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));

    assertThat(
            new DateToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(Date.from(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant()));
  }
}
