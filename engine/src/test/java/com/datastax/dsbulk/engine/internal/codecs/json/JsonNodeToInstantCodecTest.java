/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToInstantCodecTest {

  private static final Instant MILLENNIUM = Instant.parse("2000-01-01T00:00:00Z");

  private final Instant minutesAfterMillennium = MILLENNIUM.plus(Duration.ofMinutes(123456));

  private final DateTimeFormatter temporalFormat1 =
      CodecSettings.getDateTimeFormat("CQL_DATE_TIME", UTC, US, EPOCH.atZone(UTC));
  private final DateTimeFormatter temporalFormat2 =
      CodecSettings.getDateTimeFormat("yyyyMMddHHmmss", UTC, US, EPOCH.atZone(UTC));

  private final ThreadLocal<NumberFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN, true));

  @Test
  void should_convert_from_valid_input() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(temporalFormat1, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
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
        new JsonNodeToInstantCodec(temporalFormat2, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("20160724203412"))
        .to(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
    codec =
        new JsonNodeToInstantCodec(temporalFormat1, numberFormat, MINUTES, MILLENNIUM.atZone(UTC));
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("123456"))
        .to(minutesAfterMillennium)
        .convertsFrom(
            JsonNodeFactory.instance.textNode(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium)))
        .to(minutesAfterMillennium);
  }

  @Test
  void should_convert_to_valid_input() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(temporalFormat1, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
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
        new JsonNodeToInstantCodec(temporalFormat2, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec)
        .convertsTo(Instant.parse("2016-07-24T20:34:12Z"))
        .from(JsonNodeFactory.instance.textNode("20160724203412"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
    codec =
        new JsonNodeToInstantCodec(temporalFormat1, numberFormat, MINUTES, MILLENNIUM.atZone(UTC));
    // conversion back to numeric timestamps is not possible, values are always formatted with full
    // alphanumeric pattern
    assertThat(codec)
        .convertsTo(minutesAfterMillennium)
        .from(JsonNodeFactory.instance.textNode(temporalFormat1.format(minutesAfterMillennium)));
  }

  @Test
  void should_not_convert_from_invalid_input() {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(temporalFormat1, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
