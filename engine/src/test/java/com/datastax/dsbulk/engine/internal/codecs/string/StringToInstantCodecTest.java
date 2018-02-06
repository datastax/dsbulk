/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class StringToInstantCodecTest {

  private static final Instant MILLENNIUM = Instant.parse("2000-01-01T00:00:00Z");

  private final Instant minutesAfterMillennium = MILLENNIUM.plus(Duration.ofMinutes(123456));

  private final ThreadLocal<NumberFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN, true));

  @Test
  void should_convert_from_valid_input() {
    StringToInstantCodec codec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec)
        .convertsFrom("2016-07-24T20:34")
        .to(Instant.parse("2016-07-24T20:34:00Z"))
        .convertsFrom("2016-07-24T20:34:12")
        .to(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFrom("2016-07-24T20:34:12.999")
        .to(Instant.parse("2016-07-24T20:34:12.999Z"))
        .convertsFrom("2016-07-24T20:34+01:00")
        .to(Instant.parse("2016-07-24T19:34:00Z"))
        .convertsFrom("2016-07-24T20:34:12.999+01:00")
        .to(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    codec =
        new StringToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC),
            numberFormat,
            MILLISECONDS,
            EPOCH.atZone(UTC));
    assertThat(codec).convertsFrom("20160724203412").to(Instant.parse("2016-07-24T20:34:12Z"));
    codec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat, MINUTES, MILLENNIUM.atZone(UTC));
    assertThat(codec)
        .convertsFrom("123456")
        .to(minutesAfterMillennium)
        .convertsFrom(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium))
        .to(minutesAfterMillennium);
  }

  @Test
  void should_convert_to_valid_input() {
    StringToInstantCodec codec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec)
        .convertsTo(Instant.parse("2016-07-24T20:34:00Z"))
        .from("2016-07-24T20:34:00Z")
        .convertsTo(Instant.parse("2016-07-24T20:34:12Z"))
        .from("2016-07-24T20:34:12Z")
        .convertsTo(Instant.parse("2016-07-24T20:34:12.999Z"))
        .from("2016-07-24T20:34:12.999Z")
        .convertsTo(Instant.parse("2016-07-24T19:34:00Z"))
        .from("2016-07-24T19:34:00Z")
        .convertsTo(Instant.parse("2016-07-24T19:34:12.999Z"))
        .from("2016-07-24T19:34:12.999Z");
    codec =
        new StringToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC),
            numberFormat,
            MILLISECONDS,
            EPOCH.atZone(UTC));
    assertThat(codec).convertsTo(Instant.parse("2016-07-24T20:34:12Z")).from("20160724203412");
    codec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat, MINUTES, MILLENNIUM.atZone(UTC));
    // conversion back to numeric timestamps is not possible, values are always formatted with full alphanumeric pattern
    assertThat(codec)
        .convertsTo(minutesAfterMillennium)
        .from(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium));
  }

  @Test
  void should_not_convert_from_invalid_input() {
    StringToInstantCodec codec =
        new StringToInstantCodec(
            CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
