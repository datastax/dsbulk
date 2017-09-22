/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class JsonNodeToInstantCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
    JsonNodeToInstantCodec codec = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT);
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24T20:34"))
        .to(Instant.parse("2016-07-24T20:34:00Z"))
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24T20:34:12"))
        .to(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24T20:34:12.999"))
        .to(Instant.parse("2016-07-24T20:34:12.999Z"))
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24T20:34+01:00"))
        .to(Instant.parse("2016-07-24T19:34:00Z"))
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24T20:34:12.999+01:00"))
        .to(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFrom(JsonNodeFactory.instance.numberNode(1469388852999L))
        .to(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
    codec =
        new JsonNodeToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")));
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("20160724203412"))
        .to(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    JsonNodeToInstantCodec codec = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT);
    assertThat(codec)
        .convertsTo(Instant.parse("2016-07-24T20:34:00Z"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24T20:34:00Z"))
        .convertsTo(Instant.parse("2016-07-24T20:34:12Z"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24T20:34:12Z"))
        .convertsTo(Instant.parse("2016-07-24T20:34:12.999Z"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24T20:34:12.999Z"))
        .convertsTo(Instant.parse("2016-07-24T19:34:00Z"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24T19:34:00Z"))
        .convertsTo(Instant.parse("2016-07-24T19:34:12.999Z"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24T19:34:12.999Z"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
    codec =
        new JsonNodeToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")));
    assertThat(codec)
        .convertsTo(Instant.parse("2016-07-24T20:34:12Z"))
        .from(JsonNodeFactory.instance.textNode("20160724203412"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    JsonNodeToInstantCodec codec = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
