/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.driver.core.TypeCodec.timeUUID;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.tests.EngineAssertions;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class NumberToUUIDCodecTest {

  private NumberToInstantCodec<Long> instantCodec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    EngineAssertions.assertThat(new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .externalToInternal(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());
  }
}
