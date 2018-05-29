/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

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

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CqlTemporalFormat;
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
          TypeCodec.bigint(),
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CqlTemporalFormat.DEFAULT_INSTANCE,
          UTC,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(0L))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("9223372036854775807"))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-9223372036854775808"))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("9,223,372,036,854,775,807"))
        .toInternal(Long.MAX_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-9,223,372,036,854,775,808"))
        .toInternal(Long.MIN_VALUE)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal(0L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z"))
        .toInternal(946684800000L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal(1L)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal(0L)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0L)
        .toExternal(JSON_NODE_FACTORY.numberNode(0L))
        .convertsFromInternal(Long.MAX_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(9_223_372_036_854_775_807L))
        .convertsFromInternal(Long.MIN_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode(-9_223_372_036_854_775_808L))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid long"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("9223372036854775808"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("-9223372036854775809"));
  }
}
