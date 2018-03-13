/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalDateCodecTest {

  private DateTimeFormatter format1 =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_DATE", UTC, US, EPOCH.atZone(UTC));

  private DateTimeFormatter format2 =
      CodecSettings.getDateTimeFormat("yyyyMMdd", UTC, US, EPOCH.atZone(UTC));

  @Test
  void should_convert_from_valid_input() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1);
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("2016-07-24"))
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
    codec = new JsonNodeToLocalDateCodec(format2);
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("20160724"))
        .to(LocalDate.parse("2016-07-24"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1);
    assertThat(codec)
        .convertsTo(LocalDate.parse("2016-07-24"))
        .from(JSON_NODE_FACTORY.textNode("2016-07-24"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
    codec = new JsonNodeToLocalDateCodec(format2);
    assertThat(codec)
        .convertsTo(LocalDate.parse("2016-07-24"))
        .from(JSON_NODE_FACTORY.textNode("20160724"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1);
    assertThat(codec).cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
