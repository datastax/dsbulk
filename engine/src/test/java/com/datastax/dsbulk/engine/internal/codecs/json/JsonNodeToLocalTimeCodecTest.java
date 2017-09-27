/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class JsonNodeToLocalTimeCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_TIME);
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("12:24:46"))
        .to(LocalTime.parse("12:24:46"))
        .convertsFrom(JsonNodeFactory.instance.textNode("12:24:46.999"))
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
    codec = new JsonNodeToLocalTimeCodec(DateTimeFormatter.ofPattern("HHmmss.SSS"));
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("122446.999"))
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_TIME);
    assertThat(codec)
        .convertsTo(LocalTime.parse("12:24:46.999"))
        .from(JsonNodeFactory.instance.textNode("12:24:46.999"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
    codec = new JsonNodeToLocalTimeCodec(DateTimeFormatter.ofPattern("HHmmss.SSS"));
    assertThat(codec)
        .convertsTo(LocalTime.parse("12:24:46.999"))
        .from(JsonNodeFactory.instance.textNode("122446.999"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
