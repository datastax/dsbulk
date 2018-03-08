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
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
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
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class JsonNodeToLongCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final JsonNodeToLongCodec codec =
      new JsonNodeToLongCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          newArrayList("NULL"));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.numberNode(0L))
        .to(0L)
        .convertsFrom(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .to(Long.MAX_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .to(Long.MIN_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("0"))
        .to(0L)
        .convertsFrom(JSON_NODE_FACTORY.textNode("9223372036854775807"))
        .to(Long.MAX_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("-9223372036854775808"))
        .to(Long.MIN_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("9,223,372,036,854,775,807"))
        .to(Long.MAX_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("-9,223,372,036,854,775,808"))
        .to(Long.MIN_VALUE)
        .convertsFrom(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .to(0L)
        .convertsFrom(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .to(946684800000L)
        .convertsFrom(JSON_NODE_FACTORY.textNode("TRUE"))
        .to(1L)
        .convertsFrom(JSON_NODE_FACTORY.textNode("FALSE"))
        .to(0L)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode("NULL"))
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(0L)
        .from(JSON_NODE_FACTORY.numberNode(0L))
        .convertsTo(Long.MAX_VALUE)
        .from(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .convertsTo(Long.MIN_VALUE)
        .from(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .convertsTo(null)
        .from(JSON_NODE_FACTORY.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid long"))
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("9223372036854775808"))
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("-9223372036854775809"));
  }
}
