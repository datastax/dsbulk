/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.driver.core.TypeCodec.timeUUID;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.tests.EngineAssertions;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class NumberToUUIDCodecTest {

  private NumberToInstantCodec<Long> instantCodec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH);

  @Test
  void should_convert_when_valid_input() {

    EngineAssertions.assertThat(new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .convertFrom(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .convertFrom(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .convertFrom(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .convertFrom(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MIN)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, MAX)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, FIXED)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());

    assertThat(
            new NumberToUUIDCodec<>(timeUUID(), instantCodec, RANDOM)
                .convertTo(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli());
  }
}
