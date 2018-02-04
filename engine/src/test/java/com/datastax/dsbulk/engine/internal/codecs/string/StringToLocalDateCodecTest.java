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
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class StringToLocalDateCodecTest {

  @Test
  void should_convert_from_valid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .convertsFrom("2016-07-24")
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    codec = new StringToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertThat(codec).convertsFrom("20160724").to(LocalDate.parse("2016-07-24"));
  }

  @Test
  void should_convert_to_valid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("2016-07-24");
    codec = new StringToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("20160724");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
