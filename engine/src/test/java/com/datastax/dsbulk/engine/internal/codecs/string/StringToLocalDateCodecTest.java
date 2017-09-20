/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class StringToLocalDateCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
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
  public void should_convert_to_valid_input() throws Exception {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("2016-07-24");
    codec = new StringToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertThat(codec).convertsTo(LocalDate.parse("2016-07-24")).from("20160724");
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
