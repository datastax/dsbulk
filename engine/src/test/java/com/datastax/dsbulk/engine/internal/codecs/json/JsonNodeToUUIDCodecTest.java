/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonNodeToUUIDCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC));

  private final JsonNodeToUUIDCodec codec =
      new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN);

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .to(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
        .to(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX))
        .convertsFrom(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36.999999999+01:00"))
        .to(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JSON_NODE_FACTORY.textNode("123456"))
        .to(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX)
                .convertFrom(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN))
        .convertsFrom(JSON_NODE_FACTORY.numberNode(123456L))
        .to(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX)
                .convertFrom(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED)
                .convertFrom(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM)
                .convertFrom(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .from(JSON_NODE_FACTORY.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid UUID"));
  }
}
