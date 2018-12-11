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
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StringToUUIDCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(
          CodecSettings.getTemporalFormat(
              "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX",
              UTC,
              US,
              MILLISECONDS,
              EPOCH.atZone(UTC),
              numberFormat),
          UTC,
          EPOCH.atZone(UTC),
          nullStrings);

  private final StringToUUIDCodec codec =
      new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings);

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

    assertThat(new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings))
        .convertsFromExternal("2017-12-05T12:44:36+01:00")
        .toInternal(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX, nullStrings))
        .convertsFromExternal("2017-12-05T12:44:36.999999999+01:00")
        .toInternal(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED, nullStrings)
                .externalToInternal("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new StringToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM, nullStrings)
                .externalToInternal("2017-12-05T12:44:36+01:00")
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
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
