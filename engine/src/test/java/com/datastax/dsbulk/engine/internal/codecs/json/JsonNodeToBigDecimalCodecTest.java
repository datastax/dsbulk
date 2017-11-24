/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class JsonNodeToBigDecimalCodecTest {

  private final JsonNodeToBigDecimalCodec codec =
      new JsonNodeToBigDecimalCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0))
        .to(BigDecimal.ZERO)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0d))
        .to(new BigDecimal("0.0"))
        .convertsFrom(JsonNodeFactory.instance.numberNode(BigDecimal.ONE))
        .to(BigDecimal.ONE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(-1234.56))
        .to(new BigDecimal("-1234.56"))
        .convertsFrom(JsonNodeFactory.instance.textNode("-1,234.56"))
        .to(new BigDecimal("-1234.56"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.nullNode())
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(BigDecimal.ZERO)
        .from(JsonNodeFactory.instance.numberNode(BigDecimal.ZERO))
        .convertsTo(new BigDecimal("1234.56"))
        .from(JsonNodeFactory.instance.numberNode(new BigDecimal("1234.56")))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid decimal"));
  }
}
