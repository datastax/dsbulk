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
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import org.junit.jupiter.api.Test;

class JsonNodeToIntegerCodecTest {

  private final ThreadLocal<DecimalFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN));

  private final JsonNodeToIntegerCodec codec =
      new JsonNodeToIntegerCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode(0))
        .to(0)
        .convertsFrom(JsonNodeFactory.instance.numberNode(2_147_483_647))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.numberNode(-2_147_483_648))
        .to(Integer.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to(0)
        .convertsFrom(JsonNodeFactory.instance.textNode("2147483647"))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-2147483648"))
        .to(Integer.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("2,147,483,647"))
        .to(Integer.MAX_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("-2,147,483,648"))
        .to(Integer.MIN_VALUE)
        .convertsFrom(JsonNodeFactory.instance.textNode("1970-01-01T00:00:00Z"))
        .to(0)
        .convertsFrom(JsonNodeFactory.instance.textNode("TRUE"))
        .to(1)
        .convertsFrom(JsonNodeFactory.instance.textNode("FALSE"))
        .to(0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(0)
        .from(JsonNodeFactory.instance.numberNode(0))
        .convertsTo(Integer.MAX_VALUE)
        .from(JsonNodeFactory.instance.numberNode(2_147_483_647))
        .convertsTo(Integer.MIN_VALUE)
        .from(JsonNodeFactory.instance.numberNode(-2_147_483_648))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid integer"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("1.2"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("2147483648"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("-2147483649"))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
