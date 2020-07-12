/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.api.util.TimeUUIDGenerator;
import com.datastax.oss.dsbulk.codecs.text.string.StringToInstantCodec;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JsonNodeToUUIDCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = Lists.newArrayList("NULL");

  private StringToInstantCodec instantCodec =
      new StringToInstantCodec(
          CodecUtils.getTemporalFormat(
              "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX",
              UTC,
              US,
              MILLISECONDS,
              EPOCH.atZone(UTC),
              numberFormat,
              true),
          UTC,
          EPOCH.atZone(UTC),
          nullStrings);

  private final JsonNodeToUUIDCodec codec =
      new JsonNodeToUUIDCodec(TypeCodecs.UUID, instantCodec, TimeUUIDGenerator.MIN, nullStrings);

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .toInternal(UUID.fromString("a15341ec-ebef-4eab-b91d-ff16bf801a79"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);

    assertThat(
            new JsonNodeToUUIDCodec(
                TypeCodecs.UUID, instantCodec, TimeUUIDGenerator.MIN, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
        .toInternal(
            Uuids.startOf(
                ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli()));
    assertThat(
            new JsonNodeToUUIDCodec(
                TypeCodecs.UUID, instantCodec, TimeUUIDGenerator.MAX, nullStrings))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36.999999999+01:00"))
        .toInternal(
            Uuids.endOf(
                ZonedDateTime.parse("2017-12-05T12:44:36.999+01:00").toInstant().toEpochMilli()));
    assertThat(
            new JsonNodeToUUIDCodec(
                    TypeCodecs.UUID, instantCodec, TimeUUIDGenerator.FIXED, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
    assertThat(
            new JsonNodeToUUIDCodec(
                    TypeCodecs.UUID, instantCodec, TimeUUIDGenerator.RANDOM, nullStrings)
                .externalToInternal(JSON_NODE_FACTORY.textNode("2017-12-05T12:44:36+01:00"))
                .timestamp())
        .isEqualTo(
            Uuids.startOf(
                    ZonedDateTime.parse("2017-12-05T12:44:36+01:00").toInstant().toEpochMilli())
                .timestamp());
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
    assertThat(codec).cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid UUID"));
  }
}
