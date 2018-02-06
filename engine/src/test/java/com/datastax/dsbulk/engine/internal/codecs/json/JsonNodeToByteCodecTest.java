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
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class JsonNodeToByteCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final JsonNodeToByteCodec codec =
      new JsonNodeToByteCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JsonNodeFactory.instance.numberNode((byte) 0))
        .to((byte) 0)
        .convertsFrom(JsonNodeFactory.instance.numberNode((byte) 127))
        .to((byte) 127)
        .convertsFrom(JsonNodeFactory.instance.numberNode((byte) -128))
        .to((byte) -128)
        .convertsFrom(JsonNodeFactory.instance.textNode("0"))
        .to((byte) 0)
        .convertsFrom(JsonNodeFactory.instance.textNode("127"))
        .to((byte) 127)
        .convertsFrom(JsonNodeFactory.instance.textNode("-128"))
        .to((byte) -128)
        .convertsFrom(JsonNodeFactory.instance.textNode("1970-01-01T00:00:00Z"))
        .to((byte) 0)
        .convertsFrom(JsonNodeFactory.instance.textNode("TRUE"))
        .to((byte) 1)
        .convertsFrom(JsonNodeFactory.instance.textNode("FALSE"))
        .to((byte) 0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(JsonNodeFactory.instance.textNode(""))
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo((byte) 0)
        .from(JsonNodeFactory.instance.numberNode((byte) 0))
        .convertsTo((byte) 127)
        .from(JsonNodeFactory.instance.numberNode((byte) 127))
        .convertsTo((byte) -128)
        .from(JsonNodeFactory.instance.numberNode((byte) -128))
        .convertsTo(null)
        .from(JsonNodeFactory.instance.nullNode());
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("not a valid byte"))
        .cannotConvertFrom(JsonNodeFactory.instance.numberNode(1.2))
        .cannotConvertFrom(JsonNodeFactory.instance.numberNode(128))
        .cannotConvertFrom(JsonNodeFactory.instance.numberNode(-129))
        .cannotConvertFrom(JsonNodeFactory.instance.textNode("2000-01-01T00:00:00Z")) // overflow
    ;
  }
}
