/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalDateCodecTest {

  @Test
  void should_convert_from_valid_input() {
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
  void should_convert_to_valid_input() {
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
  void should_not_convert_from_invalid_input() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
