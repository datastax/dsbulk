/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.text.DecimalFormat;

public class JsonNodeToShortCodec extends JsonNodeToNumberCodec<Short> {

  public JsonNodeToShortCodec(ThreadLocal<DecimalFormat> formatter) {
    super(smallInt(), formatter);
  }

  @Override
  public Short convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isShort()) {
      return node.shortValue();
    }
    BigDecimal number = parseAsBigDecimal(node.asText());
    if (number == null) {
      return null;
    }
    return number.shortValueExact();
  }

  @Override
  public JsonNode convertTo(Short value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
