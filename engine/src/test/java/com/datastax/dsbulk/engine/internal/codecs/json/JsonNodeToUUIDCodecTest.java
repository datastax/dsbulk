/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonNodeToUUIDCodecTest {

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(CQL_DATE_TIME_FORMAT, MILLISECONDS, EPOCH);

  private final JsonNodeToUUIDCodec codec =
      new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN);

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .to(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JsonNodeFactory.instance.textNode("2017-12-05T12:44:36+01:00"))
        .to(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX))
        .convertsFrom(JsonNodeFactory.instance.textNode("2017-12-05T12:44:36.999999999+01:00"))
        .to(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JsonNodeFactory.instance.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JsonNodeFactory.instance.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JsonNodeFactory.instance.textNode("123456"))
        .to(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX)
                .convertFrom(JsonNodeFactory.instance.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JsonNodeFactory.instance.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JsonNodeFactory.instance.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JsonNodeFactory.instance.numberNode(123456L))
        .to(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX)
                .convertFrom(JsonNodeFactory.instance.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JsonNodeFactory.instance.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JsonNodeFactory.instance.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .from(JsonNodeFactory.instance.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid UUID"));
  }
}
