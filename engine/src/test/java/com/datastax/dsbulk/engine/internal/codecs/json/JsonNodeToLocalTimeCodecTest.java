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
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.US;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalTimeCodecTest {

  private DateTimeFormatter format1 =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_TIME", UTC, US, EPOCH.atZone(UTC));

  private DateTimeFormatter format2 =
      CodecSettings.getDateTimeFormat("HHmmss.SSS", UTC, US, EPOCH.atZone(UTC));

  @Test
  void should_convert_from_valid_input() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1);
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("12:24:46"))
        .to(LocalTime.parse("12:24:46"))
        .convertsFrom(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
    codec = new JsonNodeToLocalTimeCodec(format2);
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("122446.999"))
        .to(LocalTime.parse("12:24:46.999"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1);
    assertThat(codec)
        .convertsTo(LocalTime.parse("12:24:46.999"))
        .from(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
    codec = new JsonNodeToLocalTimeCodec(format2);
    assertThat(codec)
        .convertsTo(LocalTime.parse("12:24:46.999"))
        .from(JSON_NODE_FACTORY.textNode("122446.999"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_DATE);
    assertThat(codec).cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
