/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class StringToLocalTimeCodecTest {

  private DateTimeFormatter format1 =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_TIME", UTC, US, EPOCH.atZone(UTC));

  private DateTimeFormatter format2 =
      CodecSettings.getDateTimeFormat("HHmmss.SSS", UTC, US, EPOCH.atZone(UTC));

  @Test
  void should_convert_from_valid_input() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1);
    assertThat(codec)
        .convertsFrom("12:24:46")
        .to(LocalTime.parse("12:24:46"))
        .convertsFrom("12:24:46.999")
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    codec = new StringToLocalTimeCodec(format2);
    assertThat(codec).convertsFrom("122446.999").to(LocalTime.parse("12:24:46.999"));
  }

  @Test
  void should_convert_to_valid_input() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1);
    assertThat(codec).convertsTo(LocalTime.parse("12:24:46.999")).from("12:24:46.999");
    codec = new StringToLocalTimeCodec(format2);
    assertThat(codec).convertsTo(LocalTime.parse("12:24:46.999")).from("122446.999");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
