/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class JsonNodeToLocalDateCodecTest {

  @Test
  public void should_convert_from_valid_input() throws Exception {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("2016-07-24"))
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
    codec = new JsonNodeToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("20160724"))
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .convertsTo(LocalDate.parse("2016-07-24"))
        .from(JsonNodeFactory.instance.textNode("2016-07-24"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
    codec = new JsonNodeToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertThat(codec)
        .convertsTo(LocalDate.parse("2016-07-24"))
        .from(JsonNodeFactory.instance.textNode("20160724"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
