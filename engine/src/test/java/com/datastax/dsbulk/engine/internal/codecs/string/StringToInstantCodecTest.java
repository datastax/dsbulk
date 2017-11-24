/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class StringToInstantCodecTest {

  private static final Instant MILLENNIUM = Instant.parse("2000-01-01T00:00:00Z");
  private final Instant minutesAfterMillennium = MILLENNIUM.plus(Duration.ofMinutes(123456));

  @Test
  void should_convert_from_valid_input() throws Exception {
    StringToInstantCodec codec =
        new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
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
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC), MILLISECONDS, EPOCH);
    assertThat(codec).convertsFrom("20160724203412").to(Instant.parse("2016-07-24T20:34:12Z"));
    codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MINUTES, MILLENNIUM);
    assertThat(codec)
        .convertsFrom("123456")
        .to(minutesAfterMillennium)
        .convertsFrom(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium))
        .to(minutesAfterMillennium);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    StringToInstantCodec codec =
        new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
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
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC), MILLISECONDS, EPOCH);
    assertThat(codec).convertsTo(Instant.parse("2016-07-24T20:34:12Z")).from("20160724203412");
    codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MINUTES, MILLENNIUM);
    // conversion back to numeric timestamps is not possible, values are always formatted with full alphanumeric pattern
    assertThat(codec)
        .convertsTo(minutesAfterMillennium)
        .from(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium));
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    StringToInstantCodec codec =
        new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
