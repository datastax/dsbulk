/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.jdk.temporal;

import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.oss.dsbulk.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TemporalToUUIDCodecTest {

  private TemporalToTemporalCodec<ZonedDateTime, Instant> instantCodec =
      new TemporalToTemporalCodec<>(
          ZonedDateTime.class, TypeCodecs.TIMESTAMP, UTC, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    assertThat(new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")))
        .isEqualTo(
            Uuids.startOf(
                ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli()));

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")))
        .isEqualTo(
            Uuids.endOf(
                ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli()));

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00"))
                .timestamp())
        .isEqualTo(
            Uuids.endOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")
                        .toInstant()
                        .toEpochMilli())
                .timestamp());

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00"))
                .timestamp())
        .isEqualTo(
            Uuids.endOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")
                        .toInstant()
                        .toEpochMilli())
                .timestamp());

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MIN)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, MAX)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, FIXED)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    Assertions.assertThat(
            new TemporalToUUIDCodec<>(TypeCodecs.TIMEUUID, instantCodec, RANDOM)
                .internalToExternal(
                    Uuids.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));
  }
}
