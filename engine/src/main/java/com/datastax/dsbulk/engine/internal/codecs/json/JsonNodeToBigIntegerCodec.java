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
import java.math.BigInteger;
import java.text.DecimalFormat;

public class JsonNodeToBigIntegerCodec extends JsonNodeToNumberCodec<BigInteger> {

  public JsonNodeToBigIntegerCodec(ThreadLocal<DecimalFormat> formatter) {
    super(varint(), formatter);
  }

  @Override
  public BigInteger convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.bigIntegerValue();
    }
    BigDecimal number = parseAsBigDecimal(node.asText());
    if (number == null) {
      return null;
    }
    return number.toBigIntegerExact();
  }

  @Override
  public JsonNode convertTo(BigInteger value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
