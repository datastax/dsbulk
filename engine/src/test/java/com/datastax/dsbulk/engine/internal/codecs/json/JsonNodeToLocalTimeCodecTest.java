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
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalTimeCodecTest {

  @Test
  void should_convert_from_valid_input() throws Exception {
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
  void should_convert_to_valid_input() throws Exception {
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
  void should_not_convert_from_invalid_input() throws Exception {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_DATE);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
