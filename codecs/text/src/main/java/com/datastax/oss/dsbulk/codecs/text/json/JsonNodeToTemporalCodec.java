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

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.util.NumericTemporalFormat;
import com.datastax.oss.dsbulk.codecs.util.TemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class JsonNodeToTemporalCodec<T extends TemporalAccessor>
    extends JsonNodeConvertingCodec<T> {

  final TemporalFormat temporalFormat;

  JsonNodeToTemporalCodec(
      TypeCodec<T> targetCodec, TemporalFormat temporalFormat, List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.temporalFormat = temporalFormat;
  }

  @Override
  public JsonNode internalToExternal(T value) {
    if (value == null) {
      return null;
    } else if (temporalFormat instanceof NumericTemporalFormat) {
      Number n = ((NumericTemporalFormat) temporalFormat).temporalToNumber(value);
      if (n == null) {
        return null;
      }
      if (n instanceof Byte) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Byte) n);
      }
      if (n instanceof Short) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Short) n);
      }
      if (n instanceof Integer) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Integer) n);
      }
      if (n instanceof Long) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Long) n);
      }
      if (n instanceof Float) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Float) n);
      }
      if (n instanceof Double) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((Double) n);
      }
      if (n instanceof BigInteger) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((BigInteger) n);
      }
      if (n instanceof BigDecimal) {
        return JsonCodecUtils.JSON_NODE_FACTORY.numberNode((BigDecimal) n);
      }
      return JsonCodecUtils.JSON_NODE_FACTORY.textNode(n.toString());
    } else {
      return JsonCodecUtils.JSON_NODE_FACTORY.textNode(temporalFormat.format(value));
    }
  }

  TemporalAccessor parseTemporalAccessor(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (node instanceof NumericNode && temporalFormat instanceof NumericTemporalFormat) {
      Number n = node.numberValue();
      return ((NumericTemporalFormat) temporalFormat).numberToTemporal(n);
    } else {
      String s = node.asText();
      return temporalFormat.parse(s);
    }
  }
}
