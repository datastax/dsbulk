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
package com.datastax.oss.dsbulk.codecs.text.json.dse;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import java.text.ParseException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToDateRangeCodecTest {

  private List<String> nullStrings = Lists.newArrayList("NULL");
  private DateRange dateRange;

  JsonNodeToDateRangeCodecTest() {
    try {
      dateRange = DateRange.parse("[* TO 2014-12-01]");
    } catch (ParseException e) {
      // swallow; can't happen.
    }
  }

  @Test
  void should_convert_from_valid_external() {

    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("[* TO 2014-12-01]"))
        .toInternal(dateRange)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromInternal(dateRange)
        .toExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("[* TO 2014-12-01]"));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode("not a valid date range literal"));
  }
}
