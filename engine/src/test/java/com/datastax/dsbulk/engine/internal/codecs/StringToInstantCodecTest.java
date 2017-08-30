/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class StringToInstantCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
    StringToInstantCodec codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
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
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")));
    assertThat(codec).convertsFrom("20160724203412").to(Instant.parse("2016-07-24T20:34:12Z"));
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    StringToInstantCodec codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
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
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")));
    assertThat(codec).convertsTo(Instant.parse("2016-07-24T20:34:12Z")).from("20160724203412");
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    StringToInstantCodec codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
    assertThat(codec).cannotConvertFrom("not a valid date format");
  }
}
