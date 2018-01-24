/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class JsonNodeToBigDecimalCodecTest {

  private final JsonNodeToBigDecimalCodec codec =
      new JsonNodeToBigDecimalCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0))
        .to(ZERO)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0d))
        .to(new BigDecimal("0.0"))
        .convertsFrom(JsonNodeFactory.instance.numberNode(ONE))
        .to(ONE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(-1234.56))
        .to(new BigDecimal("-1234.56"))
        .convertsFrom(JsonNodeFactory.instance.textNode("-1,234.56"))
        .to(new BigDecimal("-1234.56"))
        .convertsFrom(JsonNodeFactory.instance.textNode("1970-01-01T00:00:00Z"))
        .to(new BigDecimal("0"))
        .convertsFrom(JsonNodeFactory.instance.textNode("2000-01-01T00:00:00Z"))
        .to(new BigDecimal("946684800000"))
        .convertsFrom(JsonNodeFactory.instance.textNode("TRUE"))
        .to(ONE)
        .convertsFrom(JsonNodeFactory.instance.textNode("FALSE"))
        .to(ZERO)
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
        .convertsTo(ZERO)
        .from(JsonNodeFactory.instance.numberNode(ZERO))
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
