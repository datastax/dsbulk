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
import static com.google.common.collect.Lists.newArrayList;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLocalDateCodecTest {

  private DateTimeFormatter format1 =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_DATE", UTC, US, EPOCH.atZone(UTC));

  private DateTimeFormatter format2 =
      CodecSettings.getDateTimeFormat("yyyyMMdd", UTC, US, EPOCH.atZone(UTC));

  private final List<String> nullWords = newArrayList("NULL");

  @Test
  void should_convert_from_valid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, nullWords);
    assertThat(codec)
        .convertsFrom("2016-07-24")
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("NULL")
        .to(null)
        .convertsFrom("")
        .to(null);
    codec = new StringToLocalDateCodec(format2, nullWords);
    assertThat(codec).convertsFrom("20160724").to(LocalDate.parse("2016-07-24"));
  }

  @Test
  void should_convert_to_valid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, nullWords);
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("2016-07-24");
    codec = new StringToLocalDateCodec(format2, nullWords);
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("20160724");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, nullWords);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
