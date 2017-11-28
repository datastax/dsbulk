/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

class JsonNodeToInstantCodecTest {

  private static final Instant MILLENNIUM = Instant.parse("2000-01-01T00:00:00Z");
  private final Instant minutesAfterMillennium = MILLENNIUM.plus(Duration.ofMinutes(123456));

  @Test
  void should_convert_from_valid_input() throws Exception {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
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
        new JsonNodeToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")),
            MILLISECONDS,
            EPOCH);
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("20160724203412"))
        .to(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
    codec = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MINUTES, MILLENNIUM);
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("123456"))
        .to(minutesAfterMillennium)
        .convertsFrom(
            JsonNodeFactory.instance.textNode(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium)))
        .to(minutesAfterMillennium);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
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
        new JsonNodeToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")),
            MILLISECONDS,
            EPOCH);
    assertThat(codec)
        .convertsTo(Instant.parse("2016-07-24T20:34:12Z"))
        .from(JsonNodeFactory.instance.textNode("20160724203412"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
    codec = new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MINUTES, MILLENNIUM);
    // conversion back to numeric timestamps is not possible, values are always formatted with full alphanumeric pattern
    assertThat(codec)
        .convertsTo(minutesAfterMillennium)
        .from(
            JsonNodeFactory.instance.textNode(CQL_DATE_TIME_FORMAT.format(minutesAfterMillennium)));
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    JsonNodeToInstantCodec codec =
        new JsonNodeToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid date format"));
  }
}
