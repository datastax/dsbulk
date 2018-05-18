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
import static com.google.common.collect.Lists.newArrayList;
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
import java.util.List;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonNodeToUUIDCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(
          CQL_DATE_TIME_FORMAT, numberFormat, MILLISECONDS, EPOCH.atZone(UTC), nullStrings);

  private final JsonNodeToUUIDCodec codec =
      new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings);

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .toInternal(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
        .toInternal(
            UUIDs.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36.999999999+01:00"))
        .toInternal(
            UUIDs.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("123456"))
        .toInternal(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("123456"))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());

    assertThat(new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MIN, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(123456L))
        .toInternal(UUIDs.startOf(123456L));
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, MAX, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, FIXED, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
    Assertions.assertThat(
            new JsonNodeToUUIDCodec(TypeCodec.uuid(), instantCodec, RANDOM, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.numberNode(123456L))
                .timestamp())
        .isEqualTo(UUIDs.startOf(123456L).timestamp());
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .toExternal(JSON_NODE_FACTORY.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid UUID"));
  }
}
