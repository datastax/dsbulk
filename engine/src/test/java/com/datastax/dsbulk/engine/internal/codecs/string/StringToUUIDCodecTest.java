/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StringToUUIDCodecTest {

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);

  private final StringToUUIDCodec codec =
      new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN);

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom("a15341ec-ebef-4eab-b91d-ff16bf801a79")
        .to(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);

    assertThat(new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom("2017-12-05T12:44:36+01:00")
        .to(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX))
        .convertsFrom("2017-12-05T12:44:36.999999999+01:00")
        .to(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom("123456")
        .to(UUIDs.startOf(123456L));
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX)
                .convertFrom("123456")
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom("123456")
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom("123456")
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .from("a15341ec-ebef-4eab-b91d-ff16bf801a79");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid UUID");
  }
}
