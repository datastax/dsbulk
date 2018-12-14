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
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.SimpleTemporalFormat;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalTimeCodecTest {

  private TemporalFormat format1 =
      CodecSettings.getTemporalFormat(
          "ISO_LOCAL_TIME",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

  private TemporalFormat format2 =
      CodecSettings.getTemporalFormat(
          "HHmmss.SSS",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

  private final List<String> nullStrings = newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("12:24:46"))
        .toInternal(LocalTime.parse("12:24:46"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .toInternal(LocalTime.parse("12:24:46.999"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    codec = new JsonNodeToLocalTimeCodec(format2, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("122446.999"))
        .toInternal(LocalTime.parse("12:24:46.999"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToLocalTimeCodec codec = new JsonNodeToLocalTimeCodec(format1, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal(JSON_NODE_FACTORY.textNode("12:24:46.999"))
        .convertsFromInternal(null)
        .toExternal(null);
    codec = new JsonNodeToLocalTimeCodec(format2, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal(JSON_NODE_FACTORY.textNode("122446.999"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToLocalTimeCodec codec =
        new JsonNodeToLocalTimeCodec(new SimpleTemporalFormat(ISO_LOCAL_DATE), nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
