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
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.format.temporal.TemporalFormat;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToLocalDateCodecTest {

  private TemporalFormat format1 =
      CodecUtils.getTemporalFormat(
          "ISO_LOCAL_DATE",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private TemporalFormat format2 =
      CodecUtils.getTemporalFormat(
          "yyyyMMdd",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private TemporalFormat format3 =
      CodecUtils.getTemporalFormat(
          "UNITS_SINCE_EPOCH",
          UTC,
          US,
          DAYS,
          ZonedDateTime.parse("2000-01-01T00:00:00Z"),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private final List<String> nullStrings = Lists.newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24"))
        .toInternal(LocalDate.parse("2016-07-24"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    codec = new JsonNodeToLocalDateCodec(format2, UTC, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("20160724"))
        .toInternal(LocalDate.parse("2016-07-24"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    codec = new JsonNodeToLocalDateCodec(format3, UTC, nullStrings);
    // 12 full days after year 2000 = 2000-01-13 (at midnight)
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(12))
        .toInternal(LocalDate.parse("2000-01-13"));
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalDate.parse("2016-07-24"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24"))
        .convertsFromInternal(null)
        .toExternal(null);
    codec = new JsonNodeToLocalDateCodec(format2, UTC, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalDate.parse("2016-07-24"))
        .toExternal(JSON_NODE_FACTORY.textNode("20160724"))
        .convertsFromInternal(null)
        .toExternal(null);
    codec = new JsonNodeToLocalDateCodec(format3, UTC, nullStrings);
    // 12 full days after year 2000 = 2000-01-13 (at midnight)
    assertThat(codec)
        .convertsFromInternal(LocalDate.parse("2000-01-13"))
        .toExternal(JSON_NODE_FACTORY.numberNode(12L));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToLocalDateCodec codec = new JsonNodeToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
