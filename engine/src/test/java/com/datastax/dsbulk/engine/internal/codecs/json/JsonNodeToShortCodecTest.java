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

class JsonNodeToShortCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final JsonNodeToShortCodec codec =
      new JsonNodeToShortCodec(
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
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) 0))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) 32767))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode((short) -32768))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("0"))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("32767"))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-32768"))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("32,767"))
        .toInternal((short) 32767)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("-32,768"))
        .toInternal((short) -32768)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1970-01-01T00:00:00Z"))
        .toInternal((short) 0)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("TRUE"))
        .toInternal((short) 1)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("FALSE"))
        .toInternal((short) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal((short) 0)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) 0))
        .convertsFromInternal(Short.MAX_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) 32_767))
        .convertsFromInternal(Short.MIN_VALUE)
        .toExternal(JSON_NODE_FACTORY.numberNode((short) -32_768))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid short"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("1.2"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("32768"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("-32769"))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
