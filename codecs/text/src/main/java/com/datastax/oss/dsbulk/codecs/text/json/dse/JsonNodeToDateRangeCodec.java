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

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.dsbulk.codecs.text.json.JsonNodeConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToDateRangeCodec extends JsonNodeConvertingCodec<DateRange> {

  public JsonNodeToDateRangeCodec(List<String> nullStrings) {
    super(new DateRangeCodec(), nullStrings);
  }

  @Override
  public DateRange externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    return CodecUtils.parseDateRange(node.asText());
  }

  @Override
  public JsonNode internalToExternal(DateRange value) {
    return value == null ? null : JSON_NODE_FACTORY.textNode(value.toString());
  }
}
