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
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.engine.internal.codecs.util.CqlTemporalFormat;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class JsonNodeToInstantCodecTest {

  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");

  private final Instant minutesAfterMillennium = millennium.plus(Duration.ofMinutes(123456));

  private final TemporalFormat temporalFormat1 =
      CodecSettings.getTemporalFormat("CQL_TIMESTAMP", ZoneId.of("UTC"), US);

  private final TemporalFormat temporalFormat2 =
      CodecSettings.getTemporalFormat("yyyyMMddHHmmss", ZoneId.of("UTC"), US);

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  @Test
  void should_convert_from_valid_external() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(
            temporalFormat1,
            numberFormat,
            UTC,
            MILLISECONDS,
            EPOCH.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34"))
        .toInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12"))
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999"))
        .toInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34+01:00"))
        .toInternal(Instant.parse("2016-07-24T19:34:00Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999+01:00"))
        .toInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(1469388852999L))
        .toInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    codec =
        new JsonNodeToInstantCodec(
            temporalFormat2,
            numberFormat,
            UTC,
            MILLISECONDS,
            EPOCH.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("20160724203412"))
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    codec =
        new JsonNodeToInstantCodec(
            temporalFormat1,
            numberFormat,
            UTC,
            MINUTES,
            millennium.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("123456"))
        .toInternal(minutesAfterMillennium)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(
                CqlTemporalFormat.DEFAULT_INSTANCE.format(minutesAfterMillennium)))
        .toInternal(minutesAfterMillennium);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(
            temporalFormat1,
            numberFormat,
            UTC,
            MILLISECONDS,
            EPOCH.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:00Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:00.000Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T19:34:00Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T19:34:12.999Z"))
        .convertsFromInternal(null)
        .toExternal(null);
    codec =
        new JsonNodeToInstantCodec(
            temporalFormat2,
            numberFormat,
            UTC,
            MILLISECONDS,
            EPOCH.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("20160724203412"))
        .convertsFromInternal(null)
        .toExternal(null);
    codec =
        new JsonNodeToInstantCodec(
            temporalFormat1,
            numberFormat,
            UTC,
            MINUTES,
            millennium.atZone(UTC),
            newArrayList("NULL"));
    // conversion back to numeric timestamps is not possible, values are always formatted with full
    // alphanumeric pattern
    assertThat(codec)
        .convertsFromInternal(minutesAfterMillennium)
        .toExternal(JSON_NODE_FACTORY.textNode(temporalFormat1.format(minutesAfterMillennium)));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(
            temporalFormat1,
            numberFormat,
            UTC,
            MILLISECONDS,
            EPOCH.atZone(UTC),
            newArrayList("NULL"));
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
