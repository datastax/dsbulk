/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.text.DecimalFormat;

public class JsonNodeToFloatCodec extends JsonNodeToNumberCodec<Float> {

  public JsonNodeToFloatCodec(ThreadLocal<DecimalFormat> formatter) {
    super(cfloat(), formatter);
  }

  @Override
  public Float convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.floatValue();
    }
    BigDecimal number = parseAsBigDecimal(node.asText());
    if (number == null) {
      return null;
    }
    return number.floatValue();
  }

  @Override
  public JsonNode convertTo(Float value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
