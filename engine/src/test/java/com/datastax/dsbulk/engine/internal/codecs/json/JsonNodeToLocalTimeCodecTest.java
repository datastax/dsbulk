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
import static com.google.common.collect.Lists.newArrayList;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.US;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalTimeCodecTest {

  private DateTimeFormatter format1 =
      CodecSettings.getDateTimeFormat("ISO_LOCAL_TIME", UTC, US, EPOCH.atZone(UTC));

  private DateTimeFormatter format2 =
      CodecSettings.getDateTimeFormat("HHmmss.SSS", UTC, US, EPOCH.atZone(UTC));

  private final List<String> nullWords = newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1, nullWords);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("12:24:46"))
        .toInternal(LocalTime.parse("12:24:46"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .toInternal(LocalTime.parse("12:24:46.999"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null);
    codec = new JsonNodeToLocalTimeCodec(format2, nullWords);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("122446.999"))
        .toInternal(LocalTime.parse("12:24:46.999"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1, nullWords);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .convertsFromInternal(null)
        .toExternal(JSON_NODE_FACTORY.nullNode());
    codec = new JsonNodeToLocalTimeCodec(format2, nullWords);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal(JSON_NODE_FACTORY.textNode("122446.999"))
        .convertsFromInternal(null)
        .toExternal(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(ISO_LOCAL_DATE, nullWords);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
