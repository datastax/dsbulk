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

public class JsonNodeToDoubleCodec extends JsonNodeToNumberCodec<Double> {

  public JsonNodeToDoubleCodec(ThreadLocal<DecimalFormat> formatter) {
    super(cdouble(), formatter);
  }

  @Override
  public Double convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isDouble()) {
      return node.doubleValue();
    }
    BigDecimal number = parseAsBigDecimal(node.asText());
    if (number == null) {
      return null;
    }
    return number.doubleValue();
  }

  @Override
  public JsonNode convertTo(Double value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
