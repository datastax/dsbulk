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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import org.junit.jupiter.api.Test;

class JsonNodeToDurationCodecTest {

  private final long nanosPerMinute = 60 * 1000L * 1000L * 1000L;

  private final CqlDuration duration = CqlDuration.newInstance(15, 0, 130 * nanosPerMinute);

  private final JsonNodeToDurationCodec codec =
      new JsonNodeToDurationCodec(Lists.newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1y3mo2h10m")) // standard pattern
        .toInternal(duration)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("P1Y3MT2H10M")) // ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("P0001-03-00T02:10:00")) // alternative ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(duration)
        .toExternal(JSON_NODE_FACTORY.textNode("1y3mo2h10m"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(
            JSON_NODE_FACTORY.textNode("1Y3M4D")) // The minutes should be after days
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid duration"));
  }
}
