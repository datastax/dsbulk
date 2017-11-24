/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class StringToLocalTimeCodecTest {

  @Test
  void should_convert_from_valid_input() throws Exception {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(ISO_LOCAL_TIME);
    assertThat(codec)
        .convertsFrom("12:24:46")
        .to(LocalTime.parse("12:24:46"))
        .convertsFrom("12:24:46.999")
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    codec = new StringToLocalTimeCodec(DateTimeFormatter.ofPattern("HHmmss.SSS"));
    assertThat(codec).convertsFrom("122446.999").to(LocalTime.parse("12:24:46.999"));
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(ISO_LOCAL_TIME);
    assertThat(codec).convertsTo(LocalTime.parse("12:24:46.999")).from("12:24:46.999");
    codec = new StringToLocalTimeCodec(DateTimeFormatter.ofPattern("HHmmss.SSS"));
    assertThat(codec).convertsTo(LocalTime.parse("12:24:46.999")).from("122446.999");
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(ISO_LOCAL_DATE);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
