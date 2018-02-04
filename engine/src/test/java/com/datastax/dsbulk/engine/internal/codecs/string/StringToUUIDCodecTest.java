/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.text.DecimalFormat;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StringToUUIDCodecTest {

  private final ThreadLocal<DecimalFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN));

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));

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
